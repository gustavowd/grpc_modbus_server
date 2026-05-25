use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio_modbus::client::Reader;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};

// Autogerado pelo tonic-build a partir do proto
pub mod sunspec_grpc {
    tonic::include_proto!("sunspec.telemetry.v1");
}

use sunspec_grpc::sun_spec_telemetry_service_server::{SunSpecTelemetryService, SunSpecTelemetryServiceServer};
use sunspec_grpc::{EquipmentDataResponse, Model1Common, Model213Meter, StreamRequest};
use sunspec_grpc::equipment_data_response::ModelData;

// --- 1. A ESTRUTURA DO SERVIDOR gRPC ---
pub struct TelemetryServer {
    // Canal de broadcast para enviar dados a múltiplos clientes conectados simultaneamente
    tx: broadcast::Sender<EquipmentDataResponse>,
}

#[tonic::async_trait]
impl SunSpecTelemetryService for TelemetryServer {
    type StreamEquipmentUpdatesStream = ReceiverStream<Result<EquipmentDataResponse, Status>>;

    async fn stream_equipment_updates(
        &self,
        _request: Request<StreamRequest>,
    ) -> Result<Response<Self::StreamEquipmentUpdatesStream>, Status> {
        
        // Criamos um receptor para este cliente específico
        let mut rx = self.tx.subscribe();
        // Canal mpsc interno exigido pelo Tonic para fazer o stream de saída
        let (grpc_tx, grpc_rx) = tokio::sync::mpsc::channel(128);

        // Task assíncrona que escuta o broadcast do Modbus e empurra para a rede gRPC
        tokio::spawn(async move {
            while let Ok(data) = rx.recv().await {
                if grpc_tx.send(Ok(data)).await.is_err() {
                    // Cliente desconectou, encerra a task de repasse
                    break;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(grpc_rx)))
    }
}

// Loop de leitura Modbus (SunSpec)
async fn modbus_reader_task(tx: broadcast::Sender<EquipmentDataResponse>, modbus_addr: SocketAddr) {
    use tokio_modbus::client::tcp;

    loop {
        // Tenta conectar ao inversor/medidor SunSpec Modbus TCP
        if let Ok(mut ctx) = tcp::connect(modbus_addr).await {
            println!("Conectado ao equipamento SunSpec via Modbus TCP.");

            // Leitura do modelo 1
            // Registrador base SunSpec geralmente começa em 40001 (ou offset 0 dependendo do mapeamento)
            // Vamos simular a leitura do bloco do Modelo 1 (comprimento de 66 registradores)
            if let Ok(_regs_m1) = ctx.read_input_registers(40002, 66).await {
                // No Rust real, você decodificaria a string limpando os bytes nulos:
                // let manufacturer = String::from_utf8_lossy(&regs_m1[..16]).trim().to_string();
            }

            loop {
                // Leitura modelo 213
                // Simulando a leitura do medidor trifásico e convertendo para float
                // O Modelo 213 do SunSpec usa floats de 32 bits (2 registradores por valor)
                let model_213: Option<Model213Meter> = if let Ok(Ok(regs_m213)) = ctx.read_holding_registers(40072, 60).await {
                    Some(Model213Meter {
                        ampers: u16_to_f32(regs_m213[0], regs_m213[1]),
                        ampers_phase_a: u16_to_f32(regs_m213[2], regs_m213[3]),
                        // ... demais fases ...
                        hz: 60.0,
                        real_power: u16_to_f32(regs_m213[10], regs_m213[11]),
                        ..Default::default()
                    })
                } else {
                    None
                };

                // Monta o payload gRPC padronizado
                let payload = EquipmentDataResponse {
                    equipment_id: "inversor_central_01".to_string(),
                    timestamp_ms: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("O relógio do sistema está atrasado")
                        .as_millis() as u64,
                        model_data: Some(ModelData::MeterData(model_213.unwrap_or_default())), // Aqui você pode escolher qual modelo enviar ou criar uma enum para múltiplos modelos
                        /*
                    common_data: Some(Model1Common {
                        manufacturer: "Fabricante X".to_string(),
                        model: "Modelo Y".to_string(),
                        ..Default::default()
                    }),
                    meter_data: model_213,
                     */
                };

                // Publica no barramento em memória (gRPC vai pegar isso e mandar pros clientes)
                let _ = tx.send(payload);

                let payload2 = EquipmentDataResponse {
                    equipment_id: "inversor_central_01".to_string(),
                    timestamp_ms: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("O relógio do sistema está atrasado")
                        .as_millis() as u64,
                        model_data: Some(ModelData::CommonData(Model1Common {
                            manufacturer: "Schneider".to_string(),
                            model: "iEM3000".to_string(),
                            options: "N/A".to_string(),
                            version: "1.0".to_string(),
                            serial_number: "SN-12345".to_string(),
                            da_id: 1,
                        })),
                };

                let _ = tx.send(payload2);

                // Intervalo de leitura (Ex: a cada 1 segundo)
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }

        eprintln!("Falha ao conectar ou ler o Modbus. Tentando novamente em 5 segundos...");
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

// Auxiliar para converter 2 registradores de 16-bits Modbus em um float32 do SunSpec
fn u16_to_f32(high: u16, low: u16) -> f32 {
    let bits = ((high as u32) << 16) | (low as u32);
    f32::from_bits(bits)
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configura o canal de broadcast na memória (capacidade para segurar 16 mensagens na fila se alguém atrasar)
    let (tx, _) = broadcast::channel(16);

    // Endereço fictício do seu inversor/medidor físico Modbus TCP
    let modbus_target: SocketAddr = "127.0.0.1:5502".parse()?;
    
    // Inicia a tarefa de leitura em segundo plano (Worker)
    tokio::spawn(modbus_reader_task(tx.clone(), modbus_target));

    // Endereço onde o servidor gRPC vai escutar requisições de outros softwares
    let grpc_addr: SocketAddr = "0.0.0.0:50051".parse()?;
    println!("Servidor gRPC SunSpec rodando em {}", grpc_addr);

    let telemetry_service = TelemetryServer { tx };

    Server::builder()
        .add_service(SunSpecTelemetryServiceServer::new(telemetry_service))
        .serve(grpc_addr)
        .await?;

    Ok(())
}
