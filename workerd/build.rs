fn main() {
    // Rust gets its own tonic-build codegen at compile time, independent of
    // the Go codegen path in the root Makefile's `proto` target (which only
    // emits protoc-gen-go/protoc-gen-go-grpc output into core/proto/gen —
    // Rust and Go can't share generated structs, so there's no shared
    // generated-code directory here).
    tonic_build::compile_protos("../core/proto/controlplane/v1/worker_manager.proto")
        .expect("failed to compile worker_manager.proto");
    println!("cargo:rerun-if-changed=../core/proto/controlplane/v1/worker_manager.proto");
}
