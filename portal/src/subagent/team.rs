//! Durable task -> members -> execution attempts. The Portal manager owns all
//! execution; the Desktop and model tools are clients of this same state.
use super::{ledger::now_ms, SpawnRequest, SubagentManager, TaskState, TaskStatus};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{path::PathBuf, sync::Arc};

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct TeamBook {
    pub revision: u64,
    pub tasks: Vec<TeamTask>,
    #[serde(skip)]
    path: PathBuf,
    #[serde(skip)]
    failure: Option<String>,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct TeamTask {
    pub id: String,
    pub revision: u64,
    pub title: String,
    pub description: String,
    pub scene_id: Option<String>,
    pub archived: bool,
    pub created_ms: u64,
    pub agents: Vec<Member>,
    pub items: Vec<WorkItem>,
    pub messages: Vec<Message>,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Member {
    pub id: String,
    pub name: String,
    pub brief: String,
    pub model: Option<String>,
    pub thinking: Option<String>,
    pub workdir: Option<String>,
    pub runs: Vec<Run>,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Run {
    pub id: String,
    pub status: String,
    pub created_ms: u64,
    pub ended_ms: Option<u64>,
    #[serde(default)]
    pub message_cursor: usize,
    pub result: Option<String>,
    pub error: Option<String>,
    pub turns: u32,
    pub tokens: u64,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct WorkItem {
    pub id: String,
    pub revision: u64,
    pub title: String,
    pub description: String,
    pub status: String,
    pub owner: Option<String>,
    pub blocked_by: Vec<String>,
    pub write_scopes: Vec<String>,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: String,
    pub sender: String,
    pub target: String,
    pub text: String,
    pub created_ms: u64,
}

fn identity(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::new_v4().simple())
}
fn text(args: &Value, key: &str, max: usize) -> Result<String> {
    let text = args[key].as_str().unwrap_or("").trim();
    anyhow::ensure!(
        !text.is_empty() && text.len() <= max,
        "{key} is required (maximum {max} bytes)"
    );
    Ok(text.to_owned())
}
fn optional(args: &Value, key: &str) -> Option<String> {
    args[key]
        .as_str()
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}
fn strings(args: &Value, key: &str) -> Result<Vec<String>> {
    if args[key].is_null() {
        return Ok(vec![]);
    }
    let values: Vec<String> = serde_json::from_value(args[key].clone())
        .with_context(|| format!("{key} must be an array of strings"))?;
    anyhow::ensure!(
        values.len() <= 256 && values.iter().all(|s| !s.is_empty() && s.len() <= 1024),
        "invalid {key}"
    );
    Ok(values)
}
fn active(status: &str) -> bool {
    matches!(status, "queued" | "running")
}

impl TeamBook {
    pub fn load(path: PathBuf) -> Self {
        let mut book = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<Self>(&bytes).unwrap_or_else(|e| Self {
                failure: Some(format!("Task state cannot be decoded: {e}")),
                ..Default::default()
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Self::default(),
            Err(e) => Self {
                failure: Some(e.to_string()),
                ..Default::default()
            },
        };
        book.path = path;
        // A Portal process restart cannot resume a vanished stdio process.
        for task in &mut book.tasks {
            for agent in &mut task.agents {
                for run in &mut agent.runs {
                    if active(&run.status) {
                        run.status = "interrupted".into();
                        run.ended_ms = Some(now_ms());
                    }
                }
            }
        }
        book
    }
    fn save(&mut self) -> Result<()> {
        anyhow::ensure!(
            self.failure.is_none(),
            "{}",
            self.failure.as_deref().unwrap_or("")
        );
        let result: Result<()> = (|| {
            self.revision += 1;
            std::fs::create_dir_all(self.path.parent().context("Missing task state directory")?)?;
            let temp = self.path.with_extension("tmp");
            std::fs::write(&temp, serde_json::to_vec_pretty(self)?)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&temp, std::fs::Permissions::from_mode(0o600))?;
            }
            std::fs::rename(temp, &self.path)?;
            Ok(())
        })();
        if let Err(error) = &result {
            self.failure = Some(format!("Cannot persist task state: {error}"));
        }
        result
    }
}

impl SubagentManager {
    /// `actor` is attached by the authenticated child broker, never supplied
    /// in a tool argument. Children cannot create members or cross task scopes.
    pub fn team_command<'a>(
        self: &'a Arc<Self>,
        args: Value,
        actor: Option<(&'a str, &'a str)>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value>> + Send + 'a>> {
        Box::pin(async move {
            let op = text(&args, "op", 40)?;
            if let Some((task, _)) = actor {
                anyhow::ensure!(
                    args["task_id"].as_str() == Some(task),
                    "Task scope mismatch"
                );
                anyhow::ensure!(
                    matches!(
                        op.as_str(),
                        "snapshot" | "mailbox" | "message" | "item_create" | "item_update"
                    ),
                    "Only the lead may manage members"
                );
            }
            if op == "mailbox" {
                let (task_id, agent_id) = actor.context("Mailbox requires a member identity")?;
                let book = self.team.lock().await;
                let task = book
                    .tasks
                    .iter()
                    .find(|t| t.id == task_id)
                    .context("Task not found")?;
                let after = args["after"].as_u64().unwrap_or(0) as usize;
                let messages: Vec<_> = task
                    .messages
                    .iter()
                    .skip(after)
                    .filter(|m| m.target == agent_id)
                    .collect();
                return Ok(json!({"messages":messages,"cursor":task.messages.len()}));
            }
            if op == "snapshot" {
                self.import_legacy_tasks().await?;
                let book = self.team.lock().await;
                anyhow::ensure!(
                    book.failure.is_none(),
                    "Task state unavailable: {}",
                    book.failure.as_deref().unwrap_or("")
                );
                let tasks: Vec<_> = book
                    .tasks
                    .iter()
                    .filter(|t| args["task_id"].as_str().is_none_or(|id| t.id == id))
                    .filter(|t| {
                        args["scene_id"]
                            .as_str()
                            .is_none_or(|id| t.scene_id.as_deref() == Some(id))
                    })
                    .cloned()
                    .collect();
                return Ok(
                    json!({"revision":book.revision,"tasks":tasks,"capabilities":{
                "context":"persistent-pi-session", "live_steering":false, "step_boundary_messages":true, "team_protocol":1, "engine_version":env!("CARGO_PKG_VERSION"),
                "max_concurrent":self.config.max_concurrent}}),
                );
            }
            if op == "run_agent" || op == "resume_task" {
                let budget = crate::tools::subagent::parse_budget(self.default_budget(), args.get("budget"))?;
                let task_id = text(&args, "task_id", 100)?;
                let members = {
                    let book = self.team.lock().await;
                    let task = book
                        .tasks
                        .iter()
                        .find(|t| t.id == task_id)
                        .context("Task not found")?;
                    task.agents
                        .iter()
                        .filter(|a| {
                            op == "resume_task" || args["agent_id"].as_str() == Some(a.id.as_str())
                        })
                        .filter(|a| !a.runs.last().is_some_and(|r| active(&r.status)))
                        .map(|a| a.id.clone())
                        .collect::<Vec<_>>()
                };
                anyhow::ensure!(!members.is_empty(), "No inactive member selected");
                let mut queued = vec![];
                for member in members {
                    queued.push(
                        self.queue_member(
                            &task_id,
                            &member,
                            args["message"].as_str().unwrap_or(""),
                            budget,
                        )
                        .await?,
                    );
                }
                return Ok(json!({"queued":queued}));
            }
            if op == "cancel_agent" {
                let task_id = text(&args, "task_id", 100)?;
                let agent_id = text(&args, "agent_id", 100)?;
                let mut book = self.team.lock().await;
                let task = book
                    .tasks
                    .iter_mut()
                    .find(|t| t.id == task_id)
                    .context("Task not found")?;
                let agent = task
                    .agents
                    .iter_mut()
                    .find(|a| a.id == agent_id)
                    .context("Member not found")?;
                let run = agent.runs.last_mut().context("Member has no execution")?;
                let id = run.id.clone();
                if run.status == "queued" {
                    run.status = "cancelled".into();
                    run.ended_ms = Some(now_ms());
                    book.save()?;
                    return Ok(json!({"cancelled":true}));
                }
                anyhow::ensure!(run.status == "running", "Member is not running");
                drop(book);
                return self.cancel(&id).await;
            }
            if op == "log" {
                let book = self.team.lock().await;
                let task = book
                    .tasks
                    .iter()
                    .find(|t| Some(t.id.as_str()) == args["task_id"].as_str())
                    .context("Task not found")?;
                let member = task
                    .agents
                    .iter()
                    .find(|a| Some(a.id.as_str()) == args["agent_id"].as_str())
                    .context("Member not found")?;
                let run = member.runs.last().context("No execution")?.clone();
                drop(book);
                return match self
                    .log(&run.id, args["offset"].as_u64().unwrap_or(0), 65536, 0)
                    .await
                {
                    Ok(page) => Ok(serde_json::to_value(page)?),
                    Err(_) => Ok(
                        json!({"text":run.result.unwrap_or_default(),"next_offset":0,"archived":true}),
                    ),
                };
            }
            if op == "message" {
                let task_id = text(&args, "task_id", 100)?;
                let target = text(&args, "target", 100)?;
                let message = text(&args, "message", 16384)?;
                let sender = actor.map(|(_, id)| id).unwrap_or("lead").to_string();
                anyhow::ensure!(sender != target, "Cannot message yourself");
                let mut book = self.team.lock().await;
                let task = book
                    .tasks
                    .iter_mut()
                    .find(|t| t.id == task_id)
                    .context("Task not found")?;
                anyhow::ensure!(
                    target == "lead"
                        || task
                            .agents
                            .iter()
                            .any(|a| a.id == target || a.name == target),
                    "Target not found"
                );
                let target = task
                    .agents
                    .iter()
                    .find(|a| a.id == target || a.name == target)
                    .map(|a| a.id.clone())
                    .unwrap_or(target);
                anyhow::ensure!(sender != target, "Cannot message yourself");
                let run = task
                    .agents
                    .iter()
                    .find(|a| a.id == target)
                    .and_then(|a| a.runs.last())
                    .cloned();
                let message_id = identity("msg");
                let scene_id = task.scene_id.clone();
                task.messages.push(Message {
                    id: message_id.clone(),
                    sender: sender.clone(),
                    target: target.clone(),
                    text: message.clone(),
                    created_ms: now_ms(),
                });
                task.revision += 1;
                book.save()?;
                drop(book);
                if target == "lead" {
                    self.callback.deliver_detached(message_id.clone(),json!({"source":"portal","task_id":message_id,"summary":format!("Team message from {sender}"),"result":{"kind":"subagent_message","team_task_id":task_id,"scene_id":scene_id,"sender":sender,"result":message,"portal_name":self.callback.portal_name()}}));
                }
                let delivered = run.is_some_and(|r| r.status == "running");
                // The member extension reads the mailbox before each model request.
                if args["wake"].as_bool() == Some(true) && target != "lead" {
                    let busy = {
                        let book = self.team.lock().await;
                        book.tasks
                            .iter()
                            .find(|t| t.id == task_id)
                            .and_then(|t| t.agents.iter().find(|a| a.id == target))
                            .and_then(|a| a.runs.last())
                            .is_some_and(|r| active(&r.status))
                    };
                    if !busy {
                        self.queue_member(&task_id, &target, "", self.default_budget()).await?;
                    }
                }
                return Ok(
                    json!({"accepted":true,"delivery":if delivered {"next_model_step"} else {"mailbox"}}),
                );
            }
            let mut book = self.team.lock().await;
            anyhow::ensure!(book.failure.is_none(), "Task state is invalid");
            if op == "create_task" {
                let task = TeamTask {
                    id: identity("task"),
                    revision: 1,
                    title: text(&args, "title", 200)?,
                    description: text(&args, "description", 16384)?,
                    scene_id: optional(&args, "scene_id"),
                    archived: false,
                    created_ms: now_ms(),
                    agents: vec![],
                    items: vec![],
                    messages: vec![],
                };
                let value = serde_json::to_value(&task)?;
                book.tasks.push(task);
                book.save()?;
                return Ok(value);
            }
            let task_id = text(&args, "task_id", 100)?;
            let task = book
                .tasks
                .iter_mut()
                .find(|t| t.id == task_id)
                .context("Task not found")?;
            let value = match op.as_str() {
                "add_agent" => {
                    let name = text(&args, "name", 64)?;
                    anyhow::ensure!(
                        name != "lead" && !task.agents.iter().any(|a| a.name == name),
                        "Member name already used"
                    );
                    let member = Member {
                        id: identity("agent"),
                        name,
                        brief: text(&args, "brief", 16384)?,
                        model: optional(&args, "model"),
                        thinking: optional(&args, "thinking"),
                        workdir: optional(&args, "workdir"),
                        runs: vec![],
                    };
                    let value = serde_json::to_value(&member)?;
                    task.agents.push(member);
                    value
                }
                "archive" => {
                    anyhow::ensure!(
                        !task
                            .agents
                            .iter()
                            .any(|a| a.runs.last().is_some_and(|r| active(&r.status))),
                        "Stop running members before archiving"
                    );
                    task.archived = args["archived"].as_bool().unwrap_or(true);
                    json!({"archived":task.archived})
                }
                "item_create" => {
                    let item = WorkItem {
                        id: identity("item"),
                        revision: 1,
                        title: text(&args, "title", 200)?,
                        description: text(&args, "description", 16384)?,
                        status: "pending".into(),
                        owner: None,
                        blocked_by: strings(&args, "blocked_by")?,
                        write_scopes: strings(&args, "write_scopes")?,
                    };
                    validate_graph(&task.items, &item)?;
                    let value = serde_json::to_value(&item)?;
                    task.items.push(item);
                    value
                }
                "item_update" => {
                    let index = task
                        .items
                        .iter()
                        .position(|i| Some(i.id.as_str()) == args["item_id"].as_str())
                        .context("Work item not found")?;
                    let mut item = task.items[index].clone();
                    anyhow::ensure!(
                        args["expected_revision"].as_u64() == Some(item.revision),
                        "STALE_REVISION: re-read the item"
                    );
                    let who = actor.map(|(_, id)| id).unwrap_or("lead");
                    let action = text(&args, "action", 40)?;
                    if action != "claim" {
                        anyhow::ensure!(
                            who == "lead" || item.owner.as_deref() == Some(who),
                            "Only owner or lead can modify the item"
                        );
                    }
                    match action.as_str() {
                        "claim" => {
                            anyhow::ensure!(
                                item.status == "pending" && item.owner.is_none(),
                                "Item already claimed"
                            );
                            anyhow::ensure!(
                                item.blocked_by.iter().all(|id| task
                                    .items
                                    .iter()
                                    .any(|i| i.id == *id && i.status == "completed")),
                                "Item blocked"
                            );
                            item.owner = Some(who.into());
                            item.status = "in_progress".into();
                        }
                        "complete" => {
                            anyhow::ensure!(
                                item.status == "in_progress",
                                "Item must be in progress"
                            );
                            item.status = "completed".into();
                        }
                        "release" => {
                            anyhow::ensure!(
                                item.status == "in_progress",
                                "Item must be in progress"
                            );
                            item.status = "pending".into();
                            item.owner = None;
                        }
                        "reopen" => {
                            anyhow::ensure!(item.status == "completed", "Item must be completed");
                            item.status = "pending".into();
                            item.owner = None;
                        }
                        "edit" => {
                            if !args["title"].is_null() {
                                item.title = text(&args, "title", 200)?;
                            }
                            if !args["description"].is_null() {
                                item.description = text(&args, "description", 16384)?;
                            }
                            if !args["blocked_by"].is_null() {
                                item.blocked_by = strings(&args, "blocked_by")?;
                            }
                            if !args["write_scopes"].is_null() {
                                item.write_scopes = strings(&args, "write_scopes")?;
                            }
                        }
                        "reassign" => {
                            anyhow::ensure!(who == "lead", "Lead required");
                            let owner = optional(&args, "owner");
                            anyhow::ensure!(
                                matches!(item.status.as_str(), "pending" | "in_progress"),
                                "Reopen the item before reassigning it"
                            );
                            anyhow::ensure!(
                                owner.is_none()
                                    || item.blocked_by.iter().all(|id| task
                                        .items
                                        .iter()
                                        .any(|i| i.id == *id && i.status == "completed")),
                                "Item blocked"
                            );
                            anyhow::ensure!(
                                owner.as_deref().is_none_or(
                                    |id| id == "lead" || task.agents.iter().any(|a| a.id == id)
                                ),
                                "Unknown owner"
                            );
                            item.owner = owner;
                            item.status = if item.owner.is_some() {
                                "in_progress"
                            } else {
                                "pending"
                            }
                            .into();
                        }
                        "delete" => {
                            anyhow::ensure!(
                                !task
                                    .items
                                    .iter()
                                    .any(|i| i.status != "deleted"
                                        && i.blocked_by.contains(&item.id)),
                                "Item has dependents"
                            );
                            item.status = "deleted".into();
                        }
                        _ => anyhow::bail!("Unknown item action"),
                    }
                    item.revision += 1;
                    validate_graph(&task.items, &item)?;
                    let value = serde_json::to_value(&item)?;
                    task.items[index] = item;
                    value
                }
                _ => anyhow::bail!("Unknown team operation: {op}"),
            };
            task.revision += 1;
            book.save()?;
            Ok(value)
        })
    }

    async fn queue_member(
        self: &Arc<Self>,
        task_id: &str,
        agent_id: &str,
        followup: &str,
        budget: super::Budget,
    ) -> Result<String> {
        anyhow::ensure!(
            !self.shutting_down.load(std::sync::atomic::Ordering::SeqCst),
            "Portal is shutting down"
        );
        let mut book = self.team.lock().await;
        let task = book
            .tasks
            .iter_mut()
            .find(|t| t.id == task_id)
            .context("Task not found")?;
        let agent = task
            .agents
            .iter()
            .find(|a| a.id == agent_id)
            .context("Member not found")?
            .clone();
        anyhow::ensure!(
            !agent.runs.last().is_some_and(|r| active(&r.status)),
            "Member is already running or queued"
        );
        let persisted = self.member_session_file(&agent.id).is_file();
        let mut brief = if persisted {
            format!("Continue your existing member session for task {}. Keep prior context and verify current work with portal_team snapshot. Do not treat a completed model turn as completed work.\n", task.id)
        } else {
            format!("Task: {}\n{}\n\nYou are member {} ({}). Your responsibility:\n{}\n\nUse portal_team snapshot with task_id {} to read peers, shared work items and messages. Claim ready items using their current revision; complete after verification. Use message to contact peers or lead. Check messages between steps. Write scopes are advisory. Do not mark unfinished work complete.\n\nPrior progress (historical evidence; verify against current files):\n",task.title,task.description,agent.name,agent.id,agent.brief,task.id)
        };
        anyhow::ensure!(
            followup.len() <= 16384,
            "New instruction exceeds 16384 bytes"
        );
        let history_budget =
            super::MAX_BRIEF_BYTES.saturating_sub(brief.len() + followup.len() + 512);
        let per_member = (history_budget / 2 / task.agents.len().max(1)).min(4096);
        for member in task.agents.iter().filter(|_| !persisted) {
            if let Some(latest) = member.runs.last() {
                let progress = member
                    .runs
                    .iter()
                    .rev()
                    .find_map(|r| r.result.as_deref().filter(|s| !s.is_empty()));
                let entry = format!(
                    "\n{} [latest {}]: {}\n",
                    member.name,
                    latest.status,
                    progress.unwrap_or("No saved result")
                );
                brief.push_str(&crate::heart_callback::clamp_str(&entry, per_member));
            }
        }
        let messages = task
            .messages
            .iter()
            .skip(if persisted { agent.runs.last().map_or(0, |r| r.message_cursor) } else { 0 })
            .rev()
            .filter(|m| m.target == agent.id || m.target == "lead")
            .take(30)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .map(|m| format!("\nMessage from {}: {}\n", m.sender, m.text))
            .collect::<String>();
        brief.push_str(&crate::heart_callback::clamp_str_tail(
            &messages,
            history_budget / 2,
        ));
        if !persisted {
            brief.push_str("\nHistory above is excerpted. Use portal_team snapshot for the complete saved records.\n");
        }
        if !followup.trim().is_empty() {
            brief.push_str(&format!("\nNew instruction:\n{followup}"));
        }
        let ticket = identity("queued");
        task.agents
            .iter_mut()
            .find(|a| a.id == agent_id)
            .unwrap()
            .runs
            .push(Run {
                id: ticket.clone(),
                status: "queued".into(),
                message_cursor: task.messages.len(),
                created_ms: now_ms(),
                ended_ms: None,
                result: None,
                error: None,
                turns: 0,
                tokens: 0,
            });
        task.archived = false;
        task.revision += 1;
        book.save()?;
        drop(book);
        let manager = Arc::clone(self);
        let task_id = task_id.to_owned();
        let agent_id = agent_id.to_owned();
        let queued = ticket.clone();
        tokio::spawn(async move {
            let req = SpawnRequest {
                brief,
                session: agent_id.clone(),
                workdir: agent.workdir,
                budget,
                model: agent.model,
                thinking: agent.thinking,
                scene_id: {
                    let book = manager.team.lock().await;
                    book.tasks
                        .iter()
                        .find(|t| t.id == task_id)
                        .and_then(|t| t.scene_id.clone())
                },
            };
            if let Err(error) = manager.spawn_admitted(req, Some(queued.clone())).await {
                let mut book = manager.team.lock().await;
                if let Some(run) = book
                    .tasks
                    .iter_mut()
                    .flat_map(|t| &mut t.agents)
                    .flat_map(|a| &mut a.runs)
                    .find(|r| r.id == queued && r.status == "queued")
                {
                    run.status = "failed".into();
                    run.error = Some(error.to_string());
                    run.ended_ms = Some(now_ms());
                    let _ = book.save();
                }
            }
        });
        Ok(ticket)
    }

    pub(super) async fn check_team_ticket(&self, session: &str, ticket: &str) -> Result<()> {
        let book = self.team.lock().await;
        let run = book
            .tasks
            .iter()
            .flat_map(|t| &t.agents)
            .find(|a| a.id == session)
            .and_then(|a| a.runs.last())
            .context("Missing queued member")?;
        anyhow::ensure!(
            run.id == ticket && run.status == "queued",
            "Queued execution was cancelled"
        );
        Ok(())
    }
    pub(super) async fn team_run_started(
        &self,
        task: &TaskState,
        ticket: Option<&str>,
    ) -> Result<()> {
        let mut book = self.team.lock().await;
        if let Some(member) = book
            .tasks
            .iter_mut()
            .flat_map(|t| &mut t.agents)
            .find(|a| a.id == task.session_key)
        {
            let run = member.runs.last_mut().context("Missing queued execution")?;
            anyhow::ensure!(
                run.status == "queued" && Some(run.id.as_str()) == ticket,
                "Queued execution was cancelled"
            );
            run.id = task.task_id.clone();
            run.status = "running".into();
            book.save()?;
        }
        Ok(())
    }
    pub(super) async fn team_run_finished(
        &self,
        task: &TaskState,
        status: TaskStatus,
        result: Option<&str>,
        error: Option<&str>,
        turns: u32,
        tokens: u64,
    ) {
        let mut book = self.team.lock().await;
        if let Some(run) = book
            .tasks
            .iter_mut()
            .flat_map(|t| &mut t.agents)
            .flat_map(|a| &mut a.runs)
            .find(|r| r.id == task.task_id)
        {
            run.status = status.as_str().into();
            run.result = result.map(|s| crate::heart_callback::clamp_str(s, 16384));
            run.error = error.map(str::to_owned);
            run.turns = turns;
            run.tokens = tokens;
            run.ended_ms = Some(now_ms());
            if let Err(e) = book.save() {
                tracing::error!("Cannot persist task progress: {e}");
            }
        }
    }
    pub(super) async fn team_identity(&self, session: &str) -> Option<(String, String)> {
        let book = self.team.lock().await;
        book.tasks.iter().find_map(|t| {
            t.agents
                .iter()
                .find(|a| a.id == session)
                .map(|a| (t.id.clone(), a.id.clone()))
        })
    }
    async fn import_legacy_tasks(&self) -> Result<()> {
        let records = self.ledger.lock().await.tasks.clone();
        let mut book = self.team.lock().await;
        let mut changed = false;
        for record in records {
            if book
                .tasks
                .iter()
                .flat_map(|t| &t.agents)
                .flat_map(|a| &a.runs)
                .any(|r| r.id == record.task_id)
                || record.session.starts_with("agent_")
            {
                continue;
            }
            let status = serde_json::to_value(record.status())?
                .as_str()
                .unwrap_or("interrupted")
                .to_owned();
            let brief = if record.brief.is_empty() {
                record.brief_head.clone()
            } else {
                record.brief.clone()
            };
            book.tasks.push(TeamTask {
                id: identity("task"),
                revision: 1,
                title: record.brief_head.clone(),
                description: brief.clone(),
                scene_id: record.scene_id,
                archived: false,
                created_ms: record.created_ms,
                agents: vec![Member {
                    id: identity("agent"),
                    name: "worker".into(),
                    brief,
                    model: None,
                    thinking: None,
                    workdir: Some(record.workdir),
                    runs: vec![Run {
                        id: record.task_id,
                        status,
                        message_cursor: 0,
                        created_ms: record.created_ms,
                        ended_ms: record.ended_ms,
                        result: record.result,
                        error: record.error,
                        turns: record.turns,
                        tokens: record.tokens_total,
                    }],
                }],
                items: vec![],
                messages: vec![],
            });
            changed = true;
        }
        if changed {
            book.save()?;
        }
        Ok(())
    }
}

fn validate_graph(items: &[WorkItem], candidate: &WorkItem) -> Result<()> {
    let mut unique = std::collections::HashSet::new();
    anyhow::ensure!(
        candidate.blocked_by.iter().all(|id| unique.insert(id)),
        "Duplicate dependency"
    );
    let mut items = items.to_vec();
    if let Some(i) = items.iter().position(|i| i.id == candidate.id) {
        items[i] = candidate.clone();
    } else {
        items.push(candidate.clone());
    }
    fn visit(
        id: &str,
        items: &[WorkItem],
        path: &mut Vec<String>,
        done: &mut std::collections::HashSet<String>,
    ) -> Result<()> {
        if done.contains(id) {
            return Ok(());
        }
        anyhow::ensure!(!path.iter().any(|p| p == id), "Dependency cycle");
        let item = items
            .iter()
            .find(|i| i.id == id && i.status != "deleted")
            .context("Missing dependency")?;
        path.push(id.into());
        for dependency in &item.blocked_by {
            visit(dependency, items, path, done)?;
        }
        path.pop();
        done.insert(id.into());
        Ok(())
    }
    let mut done = std::collections::HashSet::new();
    for item in &items {
        if item.status != "deleted" {
            visit(&item.id, &items, &mut vec![], &mut done)?;
        }
    }
    Ok(())
}

pub fn tool() -> crate::tools::ToolInfo {
    crate::tools::ToolInfo{name:"portal_team".into(),description:"Manage durable tasks and their named agent teams. create_task, add_agent, then run_agent or resume_task; queued runs honor the concurrency limit. snapshot shows progress, messages, shared items and revisions. Each member keeps an independent persisted pi session across runs and process restarts. Older members without a session file start from saved handoff summaries. message targets a member id/name or lead; wake:true starts an inactive member. item_create/item_update provide a shared dependency DAG with claim/release/complete/reopen/edit/reassign/delete and expected_revision. Only lead may manage members. Read snapshot after changes. Wait for required members before final answer.".into(),input_schema:json!({"type":"object","properties":{
        "op":{"type":"string","enum":["snapshot","create_task","add_agent","run_agent","resume_task","cancel_agent","message","log","archive","item_create","item_update"]},
        "budget":{"type":"object","description":"Per-run caps for run_agent/resume_task; defaults from portal.toml. 0 disables the respective limit.","properties":{"max_turns":{"type":"integer","minimum":0},"max_tokens":{"type":"integer","minimum":0},"timeout_secs":{"type":"integer","minimum":0}}},
        "task_id":{"type":"string"},"agent_id":{"type":"string"},"scene_id":{"type":"string"},"title":{"type":"string"},"description":{"type":"string"},"name":{"type":"string"},"brief":{"type":"string"},"model":{"type":"string"},"thinking":{"type":"string"},"workdir":{"type":"string"},"message":{"type":"string"},"target":{"type":"string"},"wake":{"type":"boolean"},"archived":{"type":"boolean"},"offset":{"type":"integer"},"item_id":{"type":"string"},"expected_revision":{"type":"integer"},"action":{"type":"string"},"owner":{"type":"string"},"blocked_by":{"type":"array","items":{"type":"string"}},"write_scopes":{"type":"array","items":{"type":"string"}}},"required":["op"]})}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::PortalConfig, heart_callback::HeartCallback};
    fn fixture() -> (Arc<SubagentManager>, PathBuf) {
        let dir = std::env::temp_dir().join(format!("portal-team-{}", uuid::Uuid::new_v4()));
        let mut config = PortalConfig::default();
        config.security.workspace_root = dir.join("workspace");
        config.subagent.state_dir = Some(dir.join("state").display().to_string());
        config.subagent.auto_install = false;
        (SubagentManager::new(&config, HeartCallback::new()), dir)
    }
    async fn task(manager: &Arc<SubagentManager>) -> String {
        manager
            .team_command(
                json!({"op":"create_task","title":"Build","description":"Verified work"}),
                None,
            )
            .await
            .unwrap()["id"]
            .as_str()
            .unwrap()
            .into()
    }
    async fn member(manager: &Arc<SubagentManager>, task: &str, name: &str) -> String {
        manager
            .team_command(
                json!({"op":"add_agent","task_id":task,"name":name,"brief":"Implement and verify"}),
                None,
            )
            .await
            .unwrap()["id"]
            .as_str()
            .unwrap()
            .into()
    }
    #[tokio::test]
    async fn invalid_resume_budget_does_not_queue_a_member() {
        let (manager, _dir) = fixture();
        let task_id = task(&manager).await;
        let agent_id = member(&manager, &task_id, "supervisor").await;
        let result = manager.team_command(json!({"op":"run_agent","task_id":task_id,"agent_id":agent_id,"budget":"invalid"}), None).await;
        assert!(result.is_err());
        assert!(manager.team.lock().await.tasks[0].agents[0].runs.is_empty());
    }

    #[tokio::test]
    async fn members_are_task_scoped_and_mailbox_is_incremental() {
        let (m, dir) = fixture();
        let a = task(&m).await;
        let b = task(&m).await;
        let one = member(&m, &a, "one").await;
        let two = member(&m, &a, "two").await;
        assert!(m
            .team_command(json!({"op":"snapshot","task_id":b}), Some((&a, &one)))
            .await
            .is_err());
        assert!(m
            .team_command(
                json!({"op":"add_agent","task_id":a,"name":"forged","brief":"x"}),
                Some((&a, &one))
            )
            .await
            .is_err());
        assert!(m
            .team_command(
                json!({"op":"message","task_id":a,"target":"one","message":"self"}),
                Some((&a, &one))
            )
            .await
            .is_err());
        m.team_command(
            json!({"op":"message","task_id":a,"target":"two","message":"API ready"}),
            Some((&a, &one)),
        )
        .await
        .unwrap();
        let page = m
            .team_command(
                json!({"op":"mailbox","task_id":a,"after":0}),
                Some((&a, &two)),
            )
            .await
            .unwrap();
        assert_eq!(page["messages"][0]["sender"], one);
        assert_eq!(page["messages"][0]["text"], "API ready");
        let next = m
            .team_command(
                json!({"op":"mailbox","task_id":a,"after":page["cursor"]}),
                Some((&a, &two)),
            )
            .await
            .unwrap();
        assert_eq!(next["messages"], json!([]));
        let own = m
            .team_command(json!({"op":"mailbox","task_id":a}), Some((&a, &one)))
            .await
            .unwrap();
        assert_eq!(own["messages"], json!([]));
        drop(m);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[tokio::test]
    async fn task_board_cas_dependencies_and_ownership() {
        let (m, dir) = fixture();
        let task = task(&m).await;
        let one = member(&m, &task, "one").await;
        let two = member(&m, &task, "two").await;
        let a = m
            .team_command(
                json!({"op":"item_create","task_id":task,"title":"API","description":"Ship API"}),
                None,
            )
            .await
            .unwrap();
        let b=m.team_command(json!({"op":"item_create","task_id":task,"title":"UI","description":"Use API","blocked_by":[a["id"]]}),None).await.unwrap();
        let update = |id: Value, revision: u64, action: &str| json!({"op":"item_update","task_id":task,"item_id":id,"expected_revision":revision,"action":action});
        assert!(m
            .team_command(update(b["id"].clone(), 1, "claim"), Some((&task, &one)))
            .await
            .is_err());
        m.team_command(update(a["id"].clone(), 1, "claim"), Some((&task, &one)))
            .await
            .unwrap();
        assert!(m
            .team_command(update(a["id"].clone(), 1, "claim"), Some((&task, &two)))
            .await
            .unwrap_err()
            .to_string()
            .contains("STALE_REVISION"));
        assert!(m
            .team_command(update(a["id"].clone(), 2, "complete"), Some((&task, &two)))
            .await
            .is_err());
        let mut cycle = update(a["id"].clone(), 2, "edit");
        cycle["blocked_by"] = json!([b["id"]]);
        assert!(m
            .team_command(cycle, None)
            .await
            .unwrap_err()
            .to_string()
            .contains("cycle"));
        m.team_command(update(a["id"].clone(), 2, "complete"), Some((&task, &one)))
            .await
            .unwrap();
        m.team_command(update(b["id"].clone(), 1, "claim"), Some((&task, &two)))
            .await
            .unwrap();
        assert!(m
            .team_command(update(a["id"].clone(), 3, "delete"), None)
            .await
            .is_err());
        drop(m);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[tokio::test]
    async fn restart_preserves_progress_and_interrupts_orphaned_runs() {
        let (m, dir) = fixture();
        let task = task(&m).await;
        let agent = member(&m, &task, "one").await;
        {
            let mut book = m.team.lock().await;
            book.tasks[0].agents[0].runs.push(Run {
                id: "queued_old".into(),
                status: "running".into(),
                created_ms: 1,
                ended_ms: None,
                result: Some("Verified commit progress".into()),
                message_cursor: 0,
                error: None,
                turns: 2,
                tokens: 3,
            });
            book.save().unwrap();
        }
        let loaded = TeamBook::load(dir.join("state/teams.json"));
        assert_eq!(loaded.tasks[0].agents[0].runs[0].status, "interrupted");
        assert_eq!(
            loaded.tasks[0].agents[0].runs[0].result.as_deref(),
            Some("Verified commit progress")
        );
        assert!(m.check_team_ticket(&agent, "queued_other").await.is_err());
        let mut book = m.team.lock().await;
        book.tasks[0].agents[0].runs[0].status = "queued".into();
        drop(book);
        m.check_team_ticket(&agent, "queued_old").await.unwrap();
        drop(m);
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn corrupt_state_is_not_silently_overwritten() {
        let dir =
            std::env::temp_dir().join(format!("portal-team-corrupt-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("teams.json");
        std::fs::write(&path, "{broken").unwrap();
        let mut book = TeamBook::load(path.clone());
        assert!(book.save().is_err());
        assert_eq!(std::fs::read_to_string(path).unwrap(), "{broken");
        std::fs::remove_dir_all(dir).unwrap();
    }
}
