fn main() {
    let has_sqlite = std::env::var_os("CARGO_FEATURE_STORAGE").is_some();
    let has_redb = std::env::var_os("CARGO_FEATURE_STORAGE_REDB").is_some();

    println!("cargo:rustc-check-cfg=cfg(storage_has_sqlite)");
    println!("cargo:rustc-check-cfg=cfg(storage_has_redb)");

    if has_sqlite {
        println!("cargo:rustc-cfg=storage_has_sqlite");
    }
    if has_redb {
        println!("cargo:rustc-cfg=storage_has_redb");
    }
    if !has_sqlite && !has_redb {
        println!(
            "cargo:warning=stack-graphs built without a storage backend; enable the `storage` feature for sqlite or `storage-redb` for future redb support",
        );
    }
}
