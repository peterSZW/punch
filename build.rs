use std::path::Path;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=OPENSSL_DIR");
    println!("cargo:rerun-if-env-changed=OPENSSL_LIB_DIR");
    println!("cargo:rerun-if-env-changed=PKG_CONFIG_PATH");
    println!("cargo:rerun-if-env-changed=PUNCH_OPENSSL_STATIC");
    let static_link = std::env::var("PUNCH_OPENSSL_STATIC").ok().as_deref() == Some("1");
    if let Some(dir) = env_lib_dir() {
        emit(&dir, static_link);
        return;
    }
    if let Some(dir) = pkg_config_lib_dir() {
        emit(&dir, static_link);
        return;
    }
    for dir in [
        "/opt/local/libexec/openssl3/lib",
        "/opt/homebrew/opt/openssl@3/lib",
        "/usr/local/opt/openssl@3/lib",
        "/usr/lib/x86_64-linux-gnu",
        "/usr/lib/aarch64-linux-gnu",
        "/usr/lib64",
        "/usr/lib",
    ] {
        if lib_exists(dir) {
            emit(dir, static_link);
            return;
        }
    }
    println!("cargo:rustc-link-lib=ssl");
    println!("cargo:rustc-link-lib=crypto");
}

fn env_lib_dir() -> Option<String> {
    if let Ok(dir) = std::env::var("OPENSSL_LIB_DIR") {
        if lib_exists(&dir) {
            return Some(dir);
        }
    }
    if let Ok(dir) = std::env::var("OPENSSL_DIR") {
        let lib = Path::new(&dir).join("lib");
        if lib_exists(lib.to_str()?) {
            return Some(lib.to_string_lossy().into_owned());
        }
    }
    None
}

fn pkg_config_lib_dir() -> Option<String> {
    let output = Command::new("pkg-config")
        .args(["--libs", "openssl"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    for tok in text.split_whitespace() {
        if let Some(dir) = tok.strip_prefix("-L") {
            if lib_exists(dir) {
                return Some(dir.to_string());
            }
        }
    }
    None
}

fn lib_exists(dir: &str) -> bool {
    let base = Path::new(dir);
    [
        "libssl.dylib",
        "libssl.3.dylib",
        "libssl.so",
        "libssl.so.3",
        "libssl.a",
    ]
    .iter()
    .any(|name| base.join(name).exists())
}

fn emit(dir: &str, static_link: bool) {
    println!("cargo:rustc-link-search=native={dir}");
    if static_link {
        println!("cargo:rustc-link-lib=static=ssl");
        println!("cargo:rustc-link-lib=static=crypto");
        let target = std::env::var("TARGET").unwrap_or_default();
        if target.contains("linux") {
            println!("cargo:rustc-link-lib=z");
            println!("cargo:rustc-link-lib=dl");
            println!("cargo:rustc-link-lib=pthread");
        } else if target.contains("windows") {
            println!("cargo:rustc-link-lib=dylib=ws2_32");
            println!("cargo:rustc-link-lib=dylib=gdi32");
            println!("cargo:rustc-link-lib=dylib=crypt32");
            println!("cargo:rustc-link-lib=dylib=bcrypt");
            println!("cargo:rustc-link-lib=dylib=user32");
            println!("cargo:rustc-link-lib=dylib=advapi32");
        } else if target.contains("apple") {
            println!("cargo:rustc-link-lib=z");
        }
        return;
    }
    println!("cargo:rustc-link-lib=ssl");
    println!("cargo:rustc-link-lib=crypto");
    println!("cargo:rustc-link-arg-bins=-Wl,-rpath,{dir}");
    println!("cargo:rustc-link-arg-tests=-Wl,-rpath,{dir}");
}
