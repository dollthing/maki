//! `makima --print` and SDK stream-json mode must send the `always_thinking`
//! level configured with `maki.setup`, not the session's blank default. Runs
//! the real binary against a loopback OpenAI-compatible server and inspects
//! the request body.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use serde_json::Value;

const SSE_REPLY: &str = concat!(
    "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,",
    "\"delta\":{\"role\":\"assistant\",\"content\":\"ok\"},\"finish_reason\":null}]}\n\n",
    "data: {\"id\":\"c\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,",
    "\"delta\":{},\"finish_reason\":\"stop\"}],",
    "\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\n",
    "data: [DONE]\n\n",
);

/// Serves chat completions on loopback and forwards each request body.
fn mock_server() -> (String, mpsc::Receiver<Value>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}/v1", listener.local_addr().unwrap());
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut request_line = String::new();
            if reader.read_line(&mut request_line).is_err() {
                continue;
            }
            let mut length = 0;
            loop {
                let mut header = String::new();
                if reader.read_line(&mut header).unwrap_or(0) == 0 || header == "\r\n" {
                    break;
                }
                if let Some((name, value)) = header.split_once(':')
                    && name.eq_ignore_ascii_case("content-length")
                {
                    length = value.trim().parse().unwrap_or(0);
                }
            }
            let mut body = vec![0; length];
            let _ = reader.read_exact(&mut body);
            if request_line.contains("/chat/completions") {
                if let Ok(json) = serde_json::from_slice(&body) {
                    let _ = tx.send(json);
                }
                let _ = write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n{SSE_REPLY}"
                );
            } else {
                let _ = write!(
                    stream,
                    "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                );
            }
        }
    });
    (base, rx)
}

const SDK_ARGS: &[&str] = &[
    "--input-format",
    "stream-json",
    "--output-format",
    "stream-json",
];
const SDK_TURN: &str =
    "{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"say ok\"}}\n";

/// Runs one `makima --print` turn with an isolated home and returns the first
/// chat-completions body it sent.
fn print_request(provider: &str, model: &str, setup: &str) -> Value {
    request(provider, model, setup, &["-p", "say ok"], "")
}

/// Same, through SDK mode: one stream-json user message on stdin.
fn sdk_request(provider: &str, model: &str, setup: &str) -> Value {
    request(provider, model, setup, SDK_ARGS, SDK_TURN)
}

fn request(provider: &str, model: &str, setup: &str, args: &[&str], stdin: &str) -> Value {
    let (base_url, bodies) = mock_server();
    let home = tempfile::tempdir().unwrap();
    let config = home.path().join(".config/makima");
    std::fs::create_dir_all(&config).unwrap();
    std::fs::write(
        config.join("providers.toml"),
        format!(
            "[{provider}]\nprotocol = \"openai\"\nbase_url = \"{base_url}\"\n\
             discover_models = false\n\n[[{provider}.models]]\nid = \"{model}\"\n\
             tier = \"strong\"\ncontext_window = 32768\nmax_output_tokens = 4096\n\
             supports_thinking = true\n"
        ),
    )
    .unwrap();
    std::fs::write(
        config.join("init.lua"),
        format!("maki.setup({{ {setup} }})\n"),
    )
    .unwrap();
    let work = home.path().join("work");
    std::fs::create_dir_all(&work).unwrap();

    let mut child = spawn_makima(home.path(), &work, &format!("{provider}/{model}"), args);
    // Dropping stdin closes it once the input is written.
    let _ = child.stdin.take().unwrap().write_all(stdin.as_bytes());
    let body = bodies.recv_timeout(Duration::from_secs(60));
    // An SDK session outlives its turn, so stop it rather than wait.
    let _ = child.kill();
    let output = child.wait_with_output().unwrap();
    body.unwrap_or_else(|_| {
        panic!(
            "no chat request reached the mock server; makima stderr:\n{}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

fn spawn_makima(home: &Path, work: &Path, spec: &str, args: &[&str]) -> Child {
    Command::new(env!("CARGO_BIN_EXE_makima"))
        .args(["--print", "--yolo", "--max-turns", "1", "-m", spec])
        .args(args)
        .current_dir(work)
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", home)
        .env("XDG_CONFIG_HOME", home.join(".config"))
        .env("XDG_STATE_HOME", home.join(".local/state"))
        .env("XDG_DATA_HOME", home.join(".local/share"))
        .env("XDG_CACHE_HOME", home.join(".cache"))
        .env("MOCK_API_KEY", "test")
        .env("ANANKE_API_KEY", "test")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

fn effort(body: &Value) -> (&Value, &Value) {
    (&body["reasoning_effort"], &body["thinking"])
}

#[test]
fn print_sends_always_thinking_effort() {
    let body = print_request("mock", "thinker", "always_thinking = \"high\"");
    assert_eq!(effort(&body), (&"high".into(), &Value::Null));
}

#[test]
fn print_without_always_thinking_stays_off() {
    let body = print_request("mock", "thinker", "");
    assert_eq!(effort(&body).0, "none");
}

#[test]
fn sdk_sends_always_thinking_effort() {
    let body = sdk_request("mock", "thinker", "always_thinking = \"high\"");
    assert_eq!(effort(&body), (&"high".into(), &Value::Null));
}
