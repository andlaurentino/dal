//! Shared S3-compatible object-store config for workerd's lake-touching
//! pipelines (kafkalake.rs, lakepostgres.rs). Credentials come from the
//! S3_ACCESS_KEY/S3_SECRET_KEY env vars (sourced from a k8s Secret injected
//! by controlplane's workermgr) rather than being hardcoded to one backend
//! — the same wiring works unchanged whether the datalake Connection's
//! endpoint points at RustFS, MinIO, or real AWS S3.

use std::collections::HashMap;
use std::env;

use anyhow::{Context, Result};
use object_store::aws::AmazonS3Builder;

fn access_key() -> Result<String> {
    env::var("S3_ACCESS_KEY").context("S3_ACCESS_KEY env var not set")
}

fn secret_key() -> Result<String> {
    env::var("S3_SECRET_KEY").context("S3_SECRET_KEY env var not set")
}

/// storage_options for deltalake's `open_table_with_storage_options`/
/// `DeltaOps`/`CreateBuilder` calls, which take a plain string map rather
/// than an `object_store` builder.
pub fn storage_options_for(endpoint: &str) -> Result<HashMap<String, String>> {
    let mut opts = HashMap::new();
    opts.insert("AWS_ENDPOINT_URL".to_string(), endpoint.to_string());
    opts.insert("AWS_ACCESS_KEY_ID".to_string(), access_key()?);
    opts.insert("AWS_SECRET_ACCESS_KEY".to_string(), secret_key()?);
    opts.insert("AWS_ALLOW_HTTP".to_string(), "true".to_string());
    opts.insert("AWS_S3_ALLOW_UNSAFE_RENAME".to_string(), "true".to_string());
    // Without an explicit region, object_store's S3 client falls back to
    // its default AWS credential/region discovery chain, which includes an
    // EC2 instance-metadata-service (IMDS) lookup that hangs/times out
    // off-AWS (e.g. against RustFS or MinIO). The S3-compatible store
    // ignores the region value itself, but object_store still needs one
    // set to skip that lookup entirely.
    opts.insert("AWS_REGION".to_string(), "us-east-1".to_string());
    Ok(opts)
}

/// Builds an `AmazonS3Builder` directly, for callers (lakepostgres.rs's
/// checkpoint store) that need the typed `object_store` API rather than
/// deltalake's storage_options map.
pub fn amazon_s3_builder(endpoint: &str, bucket: &str) -> Result<AmazonS3Builder> {
    Ok(AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_bucket_name(bucket)
        .with_access_key_id(access_key()?)
        .with_secret_access_key(secret_key()?)
        .with_allow_http(true)
        .with_region("us-east-1"))
}

#[cfg(test)]
mod tests {
    //! Rust half of a two-language contract test: workerd resolves S3 auth
    //! here via `AmazonS3Builder`, broker resolves the same connection's
    //! credentials via DuckDB's `CREATE SECRET`
    //! (broker/internal/executors/lake/s3_contract_test.go). Nothing in the
    //! type system enforces these two independently-implemented paths stay
    //! behaviorally identical — they drifted once already (the RustFS
    //! migration outage). Both halves read the same DAL_TEST_S3_*/
    //! S3_ACCESS_KEY/S3_SECRET_KEY env vars and must both authenticate
    //! successfully against the same running S3-compatible store.
    //!
    //! Ignored by default (needs a live store); run explicitly:
    //!
    //!   DAL_TEST_S3_ENDPOINT=http://localhost:30900 \
    //!   DAL_TEST_S3_BUCKET=dal-lake \
    //!   S3_ACCESS_KEY=dalrustfs S3_SECRET_KEY=dalrustfs123 \
    //!   cargo test --lib s3config::tests -- --ignored

    use std::env;

    use object_store::ObjectStore;

    use super::amazon_s3_builder;

    #[tokio::test]
    #[ignore]
    async fn s3_config_contract() {
        let endpoint = env::var("DAL_TEST_S3_ENDPOINT")
            .expect("DAL_TEST_S3_ENDPOINT must be set to run this test");
        let bucket =
            env::var("DAL_TEST_S3_BUCKET").expect("DAL_TEST_S3_BUCKET must be set to run this test");

        let store = amazon_s3_builder(&endpoint, &bucket)
            .expect("building AmazonS3Builder (S3_ACCESS_KEY/S3_SECRET_KEY must be set)")
            .build()
            .expect("building object store client");

        // A bare list forces authentication without requiring any object to
        // actually exist yet — it's the cheapest operation that still
        // surfaces the class of 403 InvalidAccessKeyId this test exists to
        // catch.
        use futures_util::StreamExt;
        let mut stream = store.list(None);
        match stream.next().await {
            Some(Err(e)) => panic!("listing bucket {bucket:?} failed (auth likely broken): {e}"),
            _ => {}
        }
    }
}
