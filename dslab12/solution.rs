use executor::{Handler, ModuleRef, System};
use std::collections::HashSet;
use std::collections::VecDeque;

/// Marker trait indicating that a broadcast implementation provides
/// guarantees specified in the assignment description.
pub(crate) trait ReliableBroadcast<const N: usize> {}

#[async_trait::async_trait]
pub(crate) trait ReliableBroadcastRef<const N: usize>: Send + Sync + 'static {
    async fn send(&self, msg: Operation);
}

#[async_trait::async_trait]
impl<T, const N: usize> ReliableBroadcastRef<N> for ModuleRef<T>
where
    T: ReliableBroadcast<N> + Handler<Operation> + Send,
{
    async fn send(&self, msg: Operation) {
        self.send(msg).await;
    }
}

/// Marker trait indicating that a client implementation
/// follows specification from the assignment description.
pub(crate) trait EditorClient {}

#[async_trait::async_trait]
pub(crate) trait ClientRef: Send + Sync + 'static {
    async fn send(&self, msg: Edit);
}

#[async_trait::async_trait]
impl<T> ClientRef for ModuleRef<T>
where
    T: EditorClient + Handler<Edit> + Send,
{
    async fn send(&self, msg: Edit) {
        self.send(msg).await;
    }
}

/// Actions (edits) which can be applied to a text.
#[derive(Clone)]
#[cfg_attr(test, derive(PartialEq, Debug))]
pub(crate) enum Action {
    /// Insert the character at the position.
    Insert { idx: usize, ch: char },
    /// Delete a character at the position.
    Delete { idx: usize },
    /// A _do nothing_ operation. `Nop` cannot be issued by a client.
    /// `Nop` can only be issued by a process or result from a transformation.
    Nop,
}

impl Action {
    /// Apply the action to the text.
    pub(crate) fn apply_to(&self, text: &mut String) {
        match self {
            Action::Insert { idx, ch } => {
                text.insert(*idx, *ch);
            }
            Action::Delete { idx } => {
                text.remove(*idx);
            }
            Action::Nop => {
                // Do nothing.
            }
        }
    }
}

/// Client's request to edit the text.
#[derive(Clone)]
pub(crate) struct EditRequest {
    /// Total number of operations a client has applied to its text so far.
    pub(crate) num_applied: usize,
    /// Action (edit) to be applied to a text.
    pub(crate) action: Action,
}

/// Response to a client with action (edit) it should apply to its text.
#[derive(Clone)]
pub(crate) struct Edit {
    pub(crate) action: Action,
}

#[derive(Clone)]
pub(crate) struct Operation {
    /// Rank of a process which issued this operation.
    pub(crate) process_rank: usize,
    /// Action (edit) to be applied to a text.
    pub(crate) action: Action,
}

impl Operation {
    fn transform(self, o: Operation) -> Operation {
        let new_action = match (self.action, o.action) {
            (Action::Insert { idx: idx1, ch: ch1 }, Action::Insert { idx: idx2, ch: ch2 }) => {
                //                 if p1 < p2: insert(p1, c1, r1)
                //   if p1 = p2 and r1 < r2: insert(p1, c1, r1)
                //   else: insert(p1 + 1, c1, r1)
                if idx1 < idx2 {
                    Action::Insert { idx: idx1, ch: ch1 }
                } else if idx1 == idx2 && self.process_rank < o.process_rank {
                    Action::Insert { idx: idx1, ch: ch1 }
                } else {
                    Action::Insert {
                        idx: idx1 + 1,
                        ch: ch1,
                    }
                }
            }
            (Action::Insert { idx: idx1, ch: ch1 }, Action::Delete { idx: idx2 }) => {
                // if p1 <= p2: insert(p1, c1, r1)
                // else: insert(p1 - 1, c1, r1)
                if idx1 <= idx2 {
                    Action::Insert { idx: idx1, ch: ch1 }
                } else {
                    Action::Insert {
                        idx: idx1 - 1,
                        ch: ch1,
                    }
                }
            }
            (Action::Delete { idx: idx1 }, Action::Insert { idx: idx2, ch: ch }) => {
                // if p1 < p2: delete(p1, r1)
                // if p1 = p2: NOP (do not modify text)
                // else: delete(p1 - 1, r1)
                if idx1 < idx2 {
                    Action::Delete { idx: idx1 }
                } else if idx1 == idx2 {
                    Action::Nop
                } else {
                    Action::Delete { idx: idx1 - 1 }
                }
            }
            (Action::Delete { idx: idx1 }, Action::Delete { idx: idx2 }) => {
                // if p1 < p2: delete(p1, r1)
                // else: delete(p1 + 1, r1)
                if idx1 < idx2 {
                    Action::Delete { idx: idx1 }
                } else {
                    Action::Delete { idx: idx1 + 1 }
                }
            }
            (a, _) => a,
        };
        Operation {
            process_rank: self.process_rank,
            action: new_action,
        }
    }

    // Add any methods you need.
}

/// Process of the system.
pub(crate) struct Process<const N: usize> {
    /// Rank of the process.
    rank: usize,
    /// Reference to the broadcast module.
    broadcast: Box<dyn ReliableBroadcastRef<N>>,
    /// Reference to the process's client.
    client: Box<dyn ClientRef>,
    // Add any fields you need.
    processed_during_current_round: HashSet<usize>,
    has_sent_edit_request_in_current_round: bool,
    // next_rounds[i] = processes that sent values for i-the round
    // after current in order that their messages arrived
    next_rounds: Vec<VecDeque<usize>>,
    // messages_for_next_rounds[i] = operations of i-th process
    messeges_for_next_rounds: Vec<VecDeque<Operation>>,
    edits_for_next_rounds: VecDeque<EditRequest>,
}

impl<const N: usize> Process<N> {
    pub(crate) async fn new(
        system: &mut System,
        rank: usize,
        broadcast: Box<dyn ReliableBroadcastRef<N>>,
        client: Box<dyn ClientRef>,
    ) -> ModuleRef<Self> {
        let mut messages_for_next_rounds = Vec::new();
        for _ in 0..N {
            messages_for_next_rounds.push(VecDeque::new());
        }

        let self_ref = system
            .register_module(Self {
                rank,
                broadcast,
                client,
                processed_during_current_round: HashSet::with_capacity(N),
                has_sent_edit_request_in_current_round: false,
                next_rounds: Vec::new(),
                messeges_for_next_rounds: messages_for_next_rounds,
                edits_for_next_rounds: VecDeque::new(), // Add any fields you need.
            })
            .await;
        self_ref
    }

    async fn send_nop(&mut self) {
        self.broadcast
            .send(Operation {
                process_rank: self.rank,
                action: Action::Nop,
            })
            .await;
    }

    // Add any methods you need.
}

#[async_trait::async_trait]
impl<const N: usize> Handler<Operation> for Process<N> {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: Operation) {
        // todo!("Handle operation issued by other process.");
        if !self.has_sent_edit_request_in_current_round {
            self.send_nop().await;
            todo!("I dont indent on finishing this project unfortunately!");
        }
    }
}

#[async_trait::async_trait]
impl<const N: usize> Handler<EditRequest> for Process<N> {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, request: EditRequest) {}
}
