//! Wraps the gRPC client calls a worker makes to the control plane's
//! WorkerManager service: registering on boot and reporting periodic
//! health, mirroring the old worker/internal/heartbeat.

use anyhow::Result;
use tonic::transport::Channel;

pub mod controlplane_v1 {
    tonic::include_proto!("controlplane.v1");
}

use controlplane_v1::worker_manager_client::WorkerManagerClient;
use controlplane_v1::{HeartbeatRequest, RegisterWorkerRequest, ReportErrorRequest};

#[derive(Clone)]
pub struct Client {
    rpc: WorkerManagerClient<Channel>,
}

impl Client {
    pub async fn dial(controlplane_addr: &str) -> Result<Self> {
        let endpoint = format!("http://{controlplane_addr}");
        let rpc = WorkerManagerClient::connect(endpoint).await?;
        Ok(Self { rpc })
    }

    pub async fn register(&mut self, sync_name: &str, pid: i64) -> Result<()> {
        self.rpc
            .register_worker(RegisterWorkerRequest {
                sync_name: sync_name.to_string(),
                pid,
            })
            .await?;
        Ok(())
    }

    pub async fn heartbeat(
        &mut self,
        sync_name: &str,
        phase: &str,
        consumer_lag: i64,
        watermark: &str,
        timestamp_unix: i64,
    ) -> Result<()> {
        self.rpc
            .heartbeat(HeartbeatRequest {
                sync_name: sync_name.to_string(),
                phase: phase.to_string(),
                consumer_lag,
                watermark: watermark.to_string(),
                timestamp_unix,
            })
            .await?;
        Ok(())
    }

    pub async fn report_error(&mut self, sync_name: &str, message: &str, fatal: bool) -> Result<()> {
        self.rpc
            .report_error(ReportErrorRequest {
                sync_name: sync_name.to_string(),
                message: message.to_string(),
                fatal,
            })
            .await?;
        Ok(())
    }
}
