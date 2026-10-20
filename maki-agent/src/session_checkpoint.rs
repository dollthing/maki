use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use maki_providers::{Message, ThinkingConfig, TokenUsage};
use maki_storage::checkpoint::{
    CheckpointAck, CheckpointClaim, CheckpointError, CheckpointFuture, CheckpointPublication,
    CheckpointRequest, CheckpointWriter,
};
use maki_storage::sessions::{SESSIONS_DIR, Session, SessionError, SessionLog, SessionMeta};
use maki_storage::{StateDir, StorageError, session_lock::SessionPublicationGuard};

use crate::ToolOutput;
use crate::session_coordinator::SessionCheckpoint;
use crate::session_options::{
    ENABLED_VALUE, FAST_OPTION_ID, THINKING_OPTION_ID, WORKFLOW_OPTION_ID, YOLO_OPTION_ID,
};

type StoredSession = Session<Message, TokenUsage, ToolOutput>;

const CHECKPOINT_CANCELLED: &str = "checkpoint publication cancelled";

enum SaveJob {
    Save {
        request: CheckpointRequest<SessionCheckpoint>,
        reply: flume::Sender<Result<CheckpointAck, CheckpointError>>,
    },
    Guarded {
        request: CheckpointRequest<SessionCheckpoint>,
        publication: CheckpointPublication,
        claim: CheckpointClaim,
        reply: flume::Sender<Result<CheckpointAck, CheckpointError>>,
    },
    Drain(flume::Sender<()>),
}

pub struct SessionLogCheckpoint {
    session_id: maki_storage::id::MakiId,
    save_tx: flume::Sender<SaveJob>,
    #[cfg(test)]
    session: Arc<Mutex<StoredSession>>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl SessionLogCheckpoint {
    pub fn open(
        dir: StateDir,
        session_id: maki_storage::id::MakiId,
        model: &str,
        cwd: &str,
    ) -> Result<Self, CheckpointError> {
        Self::open_inner(dir, session_id, model, cwd, None)
    }

    pub fn open_owned(
        dir: StateDir,
        session_id: maki_storage::id::MakiId,
        model: &str,
        cwd: &str,
        publication_guard: SessionPublicationGuard,
    ) -> Result<Self, CheckpointError> {
        Self::open_inner(dir, session_id, model, cwd, Some(publication_guard))
    }

    fn open_inner(
        dir: StateDir,
        session_id: maki_storage::id::MakiId,
        model: &str,
        cwd: &str,
        publication_guard: Option<SessionPublicationGuard>,
    ) -> Result<Self, CheckpointError> {
        let session = match StoredSession::load(session_id, &dir) {
            Ok(session) => session,
            Err(SessionError::Storage(StorageError::NotFound(_))) => {
                let mut session = StoredSession::new(model, cwd);
                session.id = session_id;
                session
            }
            Err(error) => {
                return Err(CheckpointError::Save {
                    session_id,
                    message: Arc::from(error.to_string()),
                });
            }
        };
        let session = Arc::new(Mutex::new(session));
        let (save_tx, save_rx) = flume::unbounded::<SaveJob>();
        let worker_session = Arc::clone(&session);
        smol::spawn(async move {
            while let Ok(job) = save_rx.recv_async().await {
                match job {
                    SaveJob::Save { request, reply } => {
                        let dir = dir.clone();
                        let session = Arc::clone(&worker_session);
                        let publication_guard = publication_guard.clone();
                        let result = smol::unblock(move || {
                            Self::save(&dir, &session, request, publication_guard.as_ref())
                        })
                        .await;
                        let _ = reply.send(result);
                    }
                    SaveJob::Guarded {
                        request,
                        publication,
                        claim,
                        reply,
                    } => {
                        let dir = dir.clone();
                        let session = Arc::clone(&worker_session);
                        let publication_guard = publication_guard.clone();
                        let result = smol::unblock(move || {
                            Self::save_guarded(
                                &dir,
                                &session,
                                request,
                                publication_guard.as_ref(),
                                &publication,
                                &claim,
                            )
                        })
                        .await;
                        let _ = reply.send(result);
                    }
                    SaveJob::Drain(reply) => {
                        let _ = reply.send(());
                    }
                }
            }
        })
        .detach();
        Ok(Self {
            session_id,
            save_tx,
            #[cfg(test)]
            session,
        })
    }

    pub fn resolve(
        session_id: maki_storage::id::MakiId,
        model: &str,
        cwd: &str,
    ) -> Result<Self, CheckpointError> {
        let dir = StateDir::resolve().map_err(|error| CheckpointError::Save {
            session_id,
            message: Arc::from(error.to_string()),
        })?;
        Self::open(dir, session_id, model, cwd)
    }

    pub async fn drain(&self) -> Result<(), CheckpointError> {
        let (reply, response) = flume::bounded(1);
        self.save_tx
            .send(SaveJob::Drain(reply))
            .map_err(|_| CheckpointError::Closed(self.session_id))?;
        response
            .recv_async()
            .await
            .map_err(|_| CheckpointError::Closed(self.session_id))
    }

    fn save(
        dir: &StateDir,
        session: &Mutex<StoredSession>,
        request: CheckpointRequest<SessionCheckpoint>,
        publication_guard: Option<&SessionPublicationGuard>,
    ) -> Result<CheckpointAck, CheckpointError> {
        let mut retained = lock(session);
        let mut candidate = retained.clone();
        apply_checkpoint(&mut candidate, &request.snapshot);
        let saved = match publication_guard {
            Some(guard) => guard
                .publish(|| candidate.save(dir))
                .map_err(|error| CheckpointError::Save {
                    session_id: request.session_id,
                    message: Arc::from(error.to_string()),
                })?
                .ok_or(CheckpointError::Closed(request.session_id))?,
            None => candidate.save(dir),
        };
        if let Err(error) = saved {
            if let Ok(published) = StoredSession::load(request.session_id, dir) {
                *retained = published;
            }
            return Err(CheckpointError::Save {
                session_id: request.session_id,
                message: Arc::from(error.to_string()),
            });
        }
        *retained = candidate;
        Ok(CheckpointAck {
            session_id: request.session_id,
            version: request.version,
        })
    }

    fn save_guarded(
        dir: &StateDir,
        session: &Mutex<StoredSession>,
        request: CheckpointRequest<SessionCheckpoint>,
        guard: Option<&SessionPublicationGuard>,
        publication: &CheckpointPublication,
        claim: &CheckpointClaim,
    ) -> Result<CheckpointAck, CheckpointError> {
        let failure = |message| CheckpointError::Save {
            session_id: request.session_id,
            message,
        };
        if publication.is_cancelled() {
            return Err(failure(Arc::from(CHECKPOINT_CANCELLED)));
        }
        let mut candidate = lock(session).clone();
        apply_checkpoint(&mut candidate, &request.snapshot);
        let directory = dir
            .ensure_subdir(SESSIONS_DIR)
            .map_err(|error| failure(Arc::from(error.to_string())))?;
        let staged = SessionLog::stage_checkpoint(&directory, &candidate)
            .map_err(|error| failure(Arc::from(error.to_string())))?;
        let mut claim_error = None;
        let mut authorized = || match claim(publication) {
            Ok(()) => true,
            Err(error) => {
                publication.cancel();
                claim_error = Some(error);
                false
            }
        };
        let published = match guard {
            Some(guard) => staged.publish(guard, authorized),
            None => staged.publish_unowned(&mut authorized),
        }
        .map_err(|error| failure(Arc::from(error.to_string())))?;
        if !published {
            publication.cancel();
            return Err(failure(
                claim_error.unwrap_or_else(|| Arc::from(CHECKPOINT_CANCELLED)),
            ));
        }
        *lock(session) = candidate;
        Ok(CheckpointAck {
            session_id: request.session_id,
            version: request.version,
        })
    }
}

impl CheckpointWriter<SessionCheckpoint> for SessionLogCheckpoint {
    fn checkpoint_guarded(
        &self,
        request: CheckpointRequest<SessionCheckpoint>,
        publication: CheckpointPublication,
        claim: CheckpointClaim,
    ) -> CheckpointFuture {
        let session_id = request.session_id;
        let (reply, response) = flume::bounded(1);
        if self
            .save_tx
            .send(SaveJob::Guarded {
                request,
                publication,
                claim,
                reply,
            })
            .is_err()
        {
            return Box::pin(async move { Err(CheckpointError::Closed(session_id)) });
        }
        Box::pin(async move {
            response
                .recv_async()
                .await
                .unwrap_or(Err(CheckpointError::Closed(session_id)))
        })
    }

    fn checkpoint(&self, request: CheckpointRequest<SessionCheckpoint>) -> CheckpointFuture {
        let session_id = request.session_id;
        let (reply, response) = flume::bounded(1);
        if self.save_tx.send(SaveJob::Save { request, reply }).is_err() {
            return Box::pin(async move { Err(CheckpointError::Closed(session_id)) });
        }
        Box::pin(async move {
            response
                .recv_async()
                .await
                .unwrap_or(Err(CheckpointError::Closed(session_id)))
        })
    }
}

fn apply_checkpoint(candidate: &mut StoredSession, checkpoint: &SessionCheckpoint) {
    if let Some(history) = &checkpoint.history {
        candidate.replace_messages(history.as_ref().clone());
    }
    candidate.set_model(checkpoint.model.to_string());
    candidate.set_cwd(checkpoint.cwd.to_string_lossy().into_owned());
    candidate.update_title_if_default();
    candidate.meta = checkpoint_meta(&candidate.meta, checkpoint);
}

fn checkpoint_meta(current: &SessionMeta, checkpoint: &SessionCheckpoint) -> SessionMeta {
    let mut meta = current.clone();
    meta.session_options.clear();
    for option in checkpoint.options.options.iter() {
        let id = option.definition.id.as_ref();
        let enabled = option.current_value.as_ref() == ENABLED_VALUE;
        match id {
            YOLO_OPTION_ID => meta.yolo = Some(enabled),
            FAST_OPTION_ID => meta.fast = enabled,
            WORKFLOW_OPTION_ID => meta.workflow = enabled,
            // Thinking predates session options and keeps its own field, so
            // the projection writes there rather than into the generic map.
            THINKING_OPTION_ID => {
                meta.thinking = option
                    .current_value
                    .parse::<ThinkingConfig>()
                    .ok()
                    .map(Into::into);
            }
            _ if option.definition.persistent && id.contains('.') => {
                meta.session_options
                    .insert(id.to_string(), option.current_value.to_string());
            }
            _ => {}
        }
    }
    meta
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use maki_storage::checkpoint::{CheckpointRequest, CheckpointVersion};
    use maki_storage::id::MakiId;
    use maki_storage::session_lock;
    use tempfile::TempDir;

    use super::*;
    use crate::session_coordinator::builtin_option_definitions;
    use crate::session_options::SessionOptions;

    const MODEL: &str = "test/model";
    const COMMITTED_MODEL: &str = "test/committed";
    const CWD: &str = "/project";
    const COMMITTED_CWD: &str = "/committed-project";
    const ORIGINAL: &str = "original";
    const TERMINAL: &str = "terminal";
    const REJECTED: &str = "rejected";
    const CLAIM_ERROR: &str = "checkpoint authority revoked";

    fn request(
        id: MakiId,
        revision: u64,
        model: &'static str,
        options: &SessionOptions,
    ) -> CheckpointRequest<SessionCheckpoint> {
        CheckpointRequest {
            session_id: id,
            version: CheckpointVersion { revision, epoch: 1 },
            snapshot: Arc::new(SessionCheckpoint {
                history: None,
                model: Arc::from(model),
                cwd: PathBuf::from("/project"),
                options: options.snapshot(),
                config: None,
            }),
        }
    }

    #[test]
    fn checkpoint_call_does_not_run_disk_save_on_the_executor() {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let id = MakiId::generate();
            let options = SessionOptions::new(
                builtin_option_definitions(
                    "test/model",
                    [Arc::from("test/model")],
                    false,
                    false,
                    false,
                    ThinkingConfig::Off,
                ),
                &BTreeMap::new(),
            )
            .unwrap();
            let writer =
                Arc::new(SessionLogCheckpoint::open(dir, id, "test/model", "/project").unwrap());
            let session_guard = lock(&writer.session);
            let (checkpoint_tx, checkpoint_rx) = flume::bounded(1);
            let checkpoint_writer = Arc::clone(&writer);
            let checkpoint_request = request(id, 1, "test/model", &options);
            let call = std::thread::spawn(move || {
                checkpoint_tx
                    .send(checkpoint_writer.checkpoint(checkpoint_request))
                    .unwrap();
            });

            let checkpoint = checkpoint_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .expect("checkpoint construction must not perform the save");
            drop(session_guard);
            call.join().unwrap();
            checkpoint.await.unwrap();
        });
    }

    #[test_case::test_case(false, false; "ordinary_history_then_config")]
    #[test_case::test_case(false, true; "ordinary_config_then_history")]
    #[test_case::test_case(true, false; "guarded_history_then_config")]
    #[test_case::test_case(true, true; "guarded_config_then_history")]
    fn concurrent_checkpoints_preserve_submission_order(guarded: bool, config_first: bool) {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let id = MakiId::generate();
            let options = SessionOptions::new(
                builtin_option_definitions(
                    MODEL,
                    [Arc::from(MODEL)],
                    false,
                    false,
                    false,
                    ThinkingConfig::Off,
                ),
                &BTreeMap::new(),
            )
            .unwrap();
            let owner = config_first.then(|| {
                session_lock::claim(&dir.ensure_subdir(SESSIONS_DIR).unwrap(), &id)
                    .unwrap()
                    .unwrap()
            });
            let writer = match &owner {
                Some(owner) => SessionLogCheckpoint::open_owned(
                    dir.clone(),
                    id,
                    MODEL,
                    CWD,
                    owner.publication_guard(),
                ),
                None => SessionLogCheckpoint::open(dir.clone(), id, MODEL, CWD),
            }
            .unwrap();
            let publication = CheckpointPublication::default();
            let claim: CheckpointClaim = Arc::new(|publication| {
                publication
                    .try_publish()
                    .then_some(())
                    .ok_or_else(|| Arc::from(CHECKPOINT_CANCELLED))
            });
            let mut config = request(id, u64::from(!config_first) + 1, COMMITTED_MODEL, &options);
            Arc::make_mut(&mut config.snapshot).cwd = COMMITTED_CWD.into();
            let mut terminal = request(id, u64::from(config_first) + 1, MODEL, &options);
            Arc::make_mut(&mut terminal.snapshot).history =
                Some(Arc::new(vec![Message::user(TERMINAL.into())]));
            let submit_config = || {
                if guarded {
                    writer.checkpoint_guarded(config, publication.clone(), Arc::clone(&claim))
                } else {
                    writer.checkpoint(config)
                }
            };
            let (first, second) = if config_first {
                (submit_config(), writer.checkpoint(terminal))
            } else {
                let first = writer.checkpoint(terminal);
                (first, submit_config())
            };
            let (first, second) = futures_lite::future::zip(first, second).await;
            for (ack, revision) in [(first.unwrap(), 1), (second.unwrap(), 2)] {
                assert_eq!(ack.session_id, id);
                assert_eq!(ack.version, CheckpointVersion { revision, epoch: 1 });
            }
            if guarded {
                assert!(!publication.cancel());
                assert_eq!(
                    writer
                        .checkpoint_guarded(request(id, 3, MODEL, &options), publication, claim)
                        .await
                        .unwrap_err(),
                    CheckpointError::Save {
                        session_id: id,
                        message: Arc::from(CHECKPOINT_CANCELLED)
                    }
                );
            }
            writer.drain().await.unwrap();
            let loaded: StoredSession = StoredSession::load(id, &dir).unwrap();
            let retained = lock(&writer.session);
            for saved in [&loaded, &*retained] {
                assert_eq!(
                    saved.model,
                    if config_first { MODEL } else { COMMITTED_MODEL }
                );
                assert_eq!(saved.cwd, if config_first { CWD } else { COMMITTED_CWD });
                assert_eq!(saved.messages()[0].first_text_content(), Some(TERMINAL));
            }
            drop(retained);
            if let Some(owner) = owner {
                owner.release().unwrap();
            }
        });
    }

    #[test_case::test_case(false, false, false; "ordinary_io_error")]
    #[test_case::test_case(true, false, false; "guarded_io_error")]
    #[test_case::test_case(true, true, false; "rejected_claim")]
    #[test_case::test_case(true, true, true; "claimed_error")]
    fn failed_candidate_is_not_retained_for_later_checkpoint(
        guarded: bool,
        claim_error: bool,
        claimed: bool,
    ) {
        let tmp = TempDir::new().unwrap();
        let dir = StateDir::from_path(tmp.path().to_path_buf());
        let id = MakiId::generate();
        let options = SessionOptions::new(
            builtin_option_definitions(
                MODEL,
                [Arc::from(MODEL)],
                false,
                false,
                false,
                ThinkingConfig::Off,
            ),
            &BTreeMap::new(),
        )
        .unwrap();
        let writer = SessionLogCheckpoint::open(dir.clone(), id, MODEL, CWD).unwrap();
        let original = Message::user(ORIGINAL.into());
        let mut accepted = request(id, 1, MODEL, &options);
        Arc::make_mut(&mut accepted.snapshot).history = Some(Arc::new(vec![original.clone()]));
        smol::block_on(writer.checkpoint(accepted)).unwrap();
        let sessions = dir.ensure_subdir(SESSIONS_DIR).unwrap();
        let path = sessions.join(format!("{id}.jsonl"));
        if !claim_error {
            std::fs::remove_file(&path).unwrap();
            std::fs::create_dir(&path).unwrap();
        }
        let mut failed = request(id, 2, COMMITTED_MODEL, &options);
        Arc::make_mut(&mut failed.snapshot).history =
            Some(Arc::new(vec![Message::user(REJECTED.into())]));
        let publication = CheckpointPublication::default();
        let failed = if guarded {
            writer.checkpoint_guarded(
                failed,
                publication.clone(),
                Arc::new(move |publication| {
                    if !claim_error || claimed {
                        assert!(publication.try_publish());
                    }
                    if claim_error {
                        Err(Arc::from(CLAIM_ERROR))
                    } else {
                        Ok(())
                    }
                }),
            )
        } else {
            writer.checkpoint(failed)
        };
        let error = smol::block_on(failed).unwrap_err();
        if claim_error {
            assert_eq!(
                error,
                CheckpointError::Save {
                    session_id: id,
                    message: Arc::from(CLAIM_ERROR)
                }
            );
            assert_eq!(publication.is_cancelled(), !claimed);
            assert_eq!(
                StoredSession::load(id, &dir).unwrap().messages()[0].first_text_content(),
                Some(ORIGINAL)
            );
        } else {
            std::fs::remove_dir_all(&path).unwrap();
        }
        {
            let retained = lock(&writer.session);
            assert_eq!(retained.model, MODEL);
            assert_eq!(retained.messages()[0].first_text_content(), Some(ORIGINAL));
        }
        smol::block_on(writer.checkpoint(request(id, 3, COMMITTED_MODEL, &options))).unwrap();
        let loaded: StoredSession = StoredSession::load(id, &dir).unwrap();
        assert_eq!(loaded.model, COMMITTED_MODEL);
        assert_eq!(
            serde_json::to_value(loaded.messages()).unwrap(),
            serde_json::to_value([original]).unwrap()
        );
    }

    #[test_case::test_case(false; "stale_owner")]
    #[test_case::test_case(true; "cancelled_before_claim")]
    fn stale_owner_cannot_publish_after_reclaim(cancelled: bool) {
        let tmp = TempDir::new().unwrap();
        let dir = StateDir::from_path(tmp.path().to_path_buf());
        let sessions = dir.ensure_subdir(SESSIONS_DIR).unwrap();
        let id = MakiId::generate();
        let old = session_lock::claim(&sessions, &id).unwrap().unwrap();
        let writer =
            SessionLogCheckpoint::open_owned(dir.clone(), id, MODEL, CWD, old.publication_guard())
                .unwrap();
        let owner = if cancelled {
            old
        } else {
            old.release().unwrap();
            session_lock::claim(&sessions, &id).unwrap().unwrap()
        };
        let options = SessionOptions::new(
            builtin_option_definitions(
                MODEL,
                [Arc::from(MODEL)],
                false,
                false,
                false,
                ThinkingConfig::Off,
            ),
            &BTreeMap::new(),
        )
        .unwrap();
        let publication = CheckpointPublication::default();
        if cancelled {
            assert!(publication.cancel());
        }
        let checkpoint = writer.checkpoint_guarded(
            request(id, 1, COMMITTED_MODEL, &options),
            publication.clone(),
            Arc::new(|_| panic!("stale publication claim ran")),
        );
        assert!(
            matches!(smol::block_on(checkpoint), Err(CheckpointError::Save { message, .. }) if message.as_ref() == CHECKPOINT_CANCELLED)
        );
        assert!(publication.is_cancelled());
        assert_eq!(lock(&writer.session).model, MODEL);
        assert!(matches!(
            StoredSession::load(id, &dir),
            Err(SessionError::Storage(StorageError::NotFound(_)))
        ));
        owner.release().unwrap();
    }

    #[test]
    fn checkpoint_errors_survive_unblock() {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let state_path = tmp.path().join("not-a-directory");
            std::fs::write(&state_path, "file").unwrap();
            let dir = StateDir::from_path(state_path);
            let id = MakiId::generate();
            assert!(matches!(
                SessionLogCheckpoint::open(dir, id, "test/model", "/project"),
                Err(CheckpointError::Save { session_id, .. }) if session_id == id
            ));
        });
    }

    #[test]
    fn corrupt_session_is_rejected_without_overwrite() {
        let tmp = TempDir::new().unwrap();
        let dir = StateDir::from_path(tmp.path().to_path_buf());
        let id = MakiId::generate();
        let mut session = StoredSession::new("test/model", "/project");
        session.id = id;
        session.save(&dir).unwrap();
        let sessions = dir
            .ensure_subdir(maki_storage::sessions::SESSIONS_DIR)
            .unwrap();
        let path = std::fs::read_dir(sessions)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "jsonl")
            })
            .unwrap();
        const CORRUPT: &[u8] = b"not json\n";
        std::fs::write(&path, CORRUPT).unwrap();

        assert!(matches!(
            SessionLogCheckpoint::open(dir, id, "test/model", "/project"),
            Err(CheckpointError::Save { session_id, .. }) if session_id == id
        ));
        assert_eq!(std::fs::read(path).unwrap(), CORRUPT);
    }

    #[test]
    fn projected_options_round_trip_session_storage() {
        smol::block_on(async {
            let tmp = TempDir::new().unwrap();
            let dir = StateDir::from_path(tmp.path().to_path_buf());
            let id = MakiId::generate();
            let options = SessionOptions::new(
                builtin_option_definitions(
                    "test/model",
                    [Arc::from("test/model")],
                    true,
                    true,
                    true,
                    ThinkingConfig::Effort(maki_providers::Effort::High),
                ),
                &BTreeMap::new(),
            )
            .unwrap();
            let writer =
                SessionLogCheckpoint::open(dir.clone(), id, "test/model", "/project").unwrap();

            writer
                .checkpoint(CheckpointRequest {
                    session_id: id,
                    version: CheckpointVersion {
                        revision: 1,
                        epoch: 1,
                    },
                    snapshot: Arc::new(SessionCheckpoint {
                        history: Some(Arc::new(vec![Message::user("hello".into())])),
                        model: Arc::from("test/model"),
                        cwd: PathBuf::from("/project"),
                        options: options.snapshot(),
                        config: None,
                    }),
                })
                .await
                .unwrap();

            let loaded: StoredSession = StoredSession::load(id, &dir).unwrap();
            assert_eq!(loaded.meta.yolo, Some(true));
            assert!(loaded.meta.fast);
            assert!(loaded.meta.workflow);
            assert_eq!(
                loaded.meta.thinking,
                Some(maki_storage::sessions::StoredThinking::Effort {
                    level: maki_providers::Effort::High
                }),
                "thinking is projected into the field it has always been saved in"
            );
            assert_eq!(loaded.messages().len(), 1);
            assert_eq!(loaded.model, "test/model");
            assert_eq!(loaded.cwd, "/project");
        });
    }
}
