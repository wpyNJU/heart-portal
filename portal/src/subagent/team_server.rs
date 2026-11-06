//! Desktop-only loopback capability for the same native team manager used by MCP.
use super::SubagentManager;
use anyhow::Result;
use serde_json::{json, Value};
use std::{path::PathBuf, sync::Arc};
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    task::{JoinHandle, JoinSet},
};

pub struct TeamServer {
    task: JoinHandle<()>,
    file: PathBuf,
}
impl TeamServer {
    pub async fn start(manager: Arc<SubagentManager>) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let token = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let file = manager
            .config
            .resolved_state_dir()
            .join("team-control.json");
        std::fs::create_dir_all(file.parent().unwrap())?;
        std::fs::write(
            &file,
            json!({"pid":std::process::id(),"port":listener.local_addr()?.port(),"token":token})
                .to_string(),
        )?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o600))?;
        }
        let task = tokio::spawn(async move {
            let mut calls = JoinSet::new();
            loop {
                tokio::select! {
                    connection=listener.accept()=>{
                        let Ok((stream,_))=connection else{break;};if calls.len()>=16{drop(stream);continue;}
                        let manager=Arc::clone(&manager);let token=token.clone();
                        calls.spawn(async move{
                            let(read,mut write)=stream.into_split();let mut line=String::new();
                            let result=tokio::time::timeout(std::time::Duration::from_secs(5),BufReader::new(read).take(1_048_577).read_line(&mut line)).await;
                            if !matches!(result,Ok(Ok(_))) || line.len()>1_048_576 || !line.ends_with('\n'){return;}
                            let Ok(value)=serde_json::from_str::<Value>(&line) else{return;};
                            if !bool::from(value["token"].as_str().unwrap_or("").as_bytes().ct_eq(token.as_bytes())){return;}
                            let result=manager.team_command(value["arguments"].clone(),None).await;
                            let response=match result{Ok(data)=>json!({"data":data}),Err(e)=>json!({"error":e.to_string()})};
                            if let Ok(mut bytes)=serde_json::to_vec(&response){bytes.push(b'\n');let _=write.write_all(&bytes).await;}
                        });
                    },
                    _=calls.join_next(),if !calls.is_empty()=>{},
                }
            }
        });
        Ok(Self { task, file })
    }
}
impl Drop for TeamServer {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_file(&self.file);
    }
}
