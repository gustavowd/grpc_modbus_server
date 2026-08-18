use std::time::Duration;
use tokio::sync::broadcast;
use tokio_modbus::client::Reader;
use tokio_modbus::client::tcp;
use tokio_stream::wrappers::{BroadcastStream, UnixListenerStream};
use tokio_stream::StreamExt;
use tonic::{transport::Server, Request, Response, Status};
use std::fs;
use tokio::net::UnixListener;

// Autogerado pelo tonic-build a partir do proto
pub mod sunspec_grpc {
    tonic::include_proto!("sunspec.telemetry.v1");
}

use sunspec_grpc::equipment_data_response::ModelData;
use sunspec_grpc::sun_spec_telemetry_service_server::{
    SunSpecTelemetryService, SunSpecTelemetryServiceServer,
};
use sunspec_grpc::{EquipmentDataResponse, Model1Common, Model213Meter, StreamRequest};

// --- ESTRUTURA DO SERVIDOR gRPC ---
pub struct TelemetryServer {
    // Canal de broadcast para enviar dados a múltiplos clientes conectados simultaneamente
    tx: broadcast::Sender<EquipmentDataResponse>,
}

#[tonic::async_trait]
impl SunSpecTelemetryService for TelemetryServer {
    // Retornamos um stream 'pinned' encadeado por combinadores
    type StreamEquipmentUpdatesStream =
        std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<EquipmentDataResponse, Status>> + Send>>;

    async fn stream_equipment_updates(
        &self,
        _request: Request<StreamRequest>,
    ) -> Result<Response<Self::StreamEquipmentUpdatesStream>, Status> {
        let rx = self.tx.subscribe();

        // Converte o broadcast em Stream tratando erros de lag de forma idiomática
        let stream = BroadcastStream::new(rx).filter_map(|res| match res {
            Ok(data) => Some(Ok(data)),
            Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(skipped)) => {
                eprintln!("Cliente gRPC perdeu {skipped} mensagens (buffer cheio)");
                None // Descarta o atraso e aguarda a próxima mensagem
            }
        });

        Ok(Response::new(Box::pin(stream)))
    }
}

// --- WORKER MODBUS ---
async fn modbus_reader_task(
    tx: broadcast::Sender<EquipmentDataResponse>,
    modbus_addr: std::net::SocketAddr,
) {
    loop {
        match tcp::connect(modbus_addr).await {
            Ok(mut ctx) => {
                println!("Conectado ao equipamento SunSpec via Modbus TCP.");

                let mut interval = tokio::time::interval(Duration::from_secs(1));

                loop {
                    interval.tick().await; // Timer preciso (melhor que sleep)

                    // Leitura do Modelo 213 (Medidor Trifásico)
                    if let Ok(Ok(regs)) = ctx.read_holding_registers(40072, 60).await {
                        let meter = Model213Meter {
                            ampers: u16_to_f32(regs[0], regs[1]),
                            ampers_phase_a: u16_to_f32(regs[2], regs[3]),
                            hz: 60.0,
                            real_power: u16_to_f32(regs[10], regs[11]),
                            ..Default::default()
                        };

                        let timestamp = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;

                        let payload = EquipmentDataResponse {
                            equipment_id: "inversor_central_01".to_string(),
                            timestamp_ms: timestamp,
                            model_data: Some(ModelData::MeterData(meter)),
                        };

                        // Publica no barramento; se não houver clientes ativos, o erro é ignorado com sucesso
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
                    } else {
                        eprintln!("Falha na leitura dos registradores Modbus.");
                        break; // Quebra o loop interno para tentar reconectar o socket
                    }
                }
            }
            Err(err) => {
                eprintln!("Erro ao conectar no Modbus: {err}. Tentando novamente em 5s...");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

// Auxiliar para converter 2 registradores de 16-bits Modbus em um float32 do SunSpec
fn u16_to_f32(high: u16, low: u16) -> f32 {
    let bits = ((high as u32) << 16) | (low as u32);
    f32::from_bits(bits)
}

// --- MAIN ---
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Canal com capacidade adequada
    let (tx, _) = broadcast::channel(64);

    // Endereço fictício do seu inversor/medidor físico Modbus TCP
    let modbus_target = "127.0.0.1:5502".parse()?;

    // Inicia a tarefa de leitura em segundo plano (Worker)
    tokio::spawn(modbus_reader_task(tx.clone(), modbus_target));

    let socket_path = "/tmp/sunspec.sock";

    // Remoção limpa do socket antigo caso exista
    let _ = fs::remove_file(socket_path);

    // Criamos o escutador Unix nativo do Tokio
    let uds = UnixListener::bind(socket_path)?;
    let uds_stream = UnixListenerStream::new(uds);

    println!("Servidor gRPC rodando via UDS em: {socket_path}");

    Server::builder()
        .add_service(SunSpecTelemetryServiceServer::new(TelemetryServer { tx }))
        .serve_with_incoming(uds_stream)
        .await?;

    Ok(())
}
