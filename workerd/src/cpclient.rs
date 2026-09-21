//! Thin HTTP client mirroring the old worker/internal/cpclient: fetches this
//! worker's own Sync spec and the source/target Connections it references
//! from the control plane's REST API (controlplane/internal/httpapi), since
//! the gRPC WorkerManager service only carries heartbeat/registration
//! traffic.

use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use serde::Deserialize;

use crate::types::{ConnectionSpec, SyncSpec};

#[derive(Deserialize)]
struct Envelope<T> {
    spec: T,
}

async fn get<T: DeserializeOwned>(base_url: &str, path: &str) -> Result<T> {
    let url = format!("{base_url}{path}");
    let res = reqwest::get(&url).await.with_context(|| format!("GET {url}"))?;
    let status = res.status();
    if !status.is_success() {
        anyhow::bail!("GET {path}: unexpected status {status}");
    }
    let env: Envelope<T> = res
        .json()
        .await
        .with_context(|| format!("GET {path}: decoding response"))?;
    Ok(env.spec)
}

pub async fn get_sync(base_url: &str, name: &str) -> Result<SyncSpec> {
    get(base_url, &format!("/api/v1alpha1/syncs/{name}")).await
}

pub async fn get_connection(base_url: &str, name: &str) -> Result<ConnectionSpec> {
    get(base_url, &format!("/api/v1alpha1/connections/{name}")).await
}
