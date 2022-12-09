use async_channel::Sender;
use executor::{Handler, ModuleRef};
use std::future::Future;
use std::mem;
use std::pin::Pin;
use uuid::Uuid;

#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Debug)]
pub(crate) enum ProductType {
    Electronics,
    Toys,
    Books,
}

#[derive(Clone)]
pub(crate) struct StoreMsg {
    sender: ModuleRef<CyberStore2047>,
    content: StoreMsgContent,
}

#[derive(Copy, Clone, Debug)]
enum StoreState {
    VoteCouting(u32, bool),
    Finalizing(u32, TwoPhaseResult),
    Init,
}

#[derive(Clone, Debug)]
pub(crate) enum StoreMsgContent {
    /// Transaction Manager initiates voting for the transaction.
    RequestVote(Transaction),
    /// If every process is ok with transaction, TM issues commit.
    Commit,
    /// System-wide abort.
    Abort,
}

#[derive(Clone)]
pub(crate) struct NodeMsg {
    content: NodeMsgContent,
}

#[derive(Clone, Debug)]
pub(crate) enum NodeMsgContent {
    /// Process replies to TM whether it can/cannot commit the transaction.
    RequestVoteResponse(TwoPhaseResult),
    /// Process acknowledges to TM committing/aborting the transaction.
    FinalizationAck,
}

type CompletedCallBack =
    Box<dyn FnOnce(TwoPhaseResult) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

pub(crate) struct TransactionMessage {
    /// Request to change price.
    pub(crate) transaction: Transaction,

    /// Called after 2PC completes (i.e., the transaction was decided to be
    /// committed/aborted by CyberStore2047). This must be called after responses
    /// from all processes acknowledging commit or abort are collected.
    pub(crate) completed_callback:
        Box<dyn FnOnce(TwoPhaseResult) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum TwoPhaseResult {
    Ok,
    Abort,
}

#[derive(Copy, Clone)]
pub(crate) struct Product {
    pub(crate) identifier: Uuid,
    pub(crate) pr_type: ProductType,
    pub(crate) price: u64,
}

#[derive(Copy, Clone, Debug)]
pub(crate) struct Transaction {
    pub(crate) pr_type: ProductType,
    pub(crate) shift: i32,
}

pub(crate) struct ProductPriceQuery {
    pub(crate) product_ident: Uuid,
    pub(crate) result_sender: Sender<ProductPrice>,
}

pub(crate) struct ProductPrice(pub(crate) Option<u64>);

/// Message which disables a node. Used for testing.
pub(crate) struct Disable;

/// CyberStore2047.
/// This structure serves as TM.
// Add any fields you need.
pub(crate) struct CyberStore2047 {
    nodes: Vec<ModuleRef<Node>>,
    n_nodes: u32,
    state: StoreState,
    transaction_completed_callback: Option<CompletedCallBack>,
}

impl CyberStore2047 {
    pub(crate) fn new(nodes: Vec<ModuleRef<Node>>) -> Self {
        return CyberStore2047 {
            state: StoreState::Init,
            n_nodes: nodes.len() as u32,
            nodes: nodes,
            transaction_completed_callback: None,
        };
    }
}

/// Node of CyberStore2047.
/// This structure serves as a process of the distributed system.
// Add any fields you need.
pub(crate) struct Node {
    products: Vec<Product>,
    pending_transaction: Option<Transaction>,
    enabled: bool,
}

impl Node {
    pub(crate) fn new(products: Vec<Product>) -> Self {
        Self {
            products,
            pending_transaction: None,
            enabled: true,
        }
    }
}

fn check_if_voting_end(n_votes: u32, has_aborted: bool, n_nodes: u32) -> Option<TwoPhaseResult> {
    if n_votes == n_nodes {
        let result = if has_aborted {
            TwoPhaseResult::Abort
        } else {
            TwoPhaseResult::Ok
        };
        Some(result)
    } else {
        None
    }
}

fn add_vote(store: &mut CyberStore2047, t: TwoPhaseResult) -> Option<(u32, bool)> {
    if let StoreState::VoteCouting(n_votes, has_aborted) = store.state {
        let has_aborted_new = match t {
            TwoPhaseResult::Abort => true,
            _ => has_aborted,
        };

        store.state = StoreState::VoteCouting(n_votes + 1, has_aborted_new);
        Some((n_votes + 1, has_aborted_new))
    } else {
        None
    }
}

#[async_trait::async_trait]
impl Handler<NodeMsg> for CyberStore2047 {
    async fn handle(&mut self, self_ref: &ModuleRef<Self>, msg: NodeMsg) {
        match (msg.content, self.state.clone()) {
            (NodeMsgContent::RequestVoteResponse(t), StoreState::VoteCouting(_, _)) => {
                log::debug!("Vote response {:?}", t.clone());
                let (new_n_votes, new_has_aborted) = add_vote(self, t).unwrap();
                log::debug!("All votes = {:?}", self.state);
                if let Some(result) =
                    check_if_voting_end(new_n_votes, new_has_aborted, self.n_nodes)
                {
                    let new_store_msg_content = if result == TwoPhaseResult::Abort {
                        StoreMsgContent::Abort
                    } else {
                        StoreMsgContent::Commit
                    };
                    log::debug!("Sending voting end! {:?}", result.clone());
                    for node in self.nodes.iter() {
                        node.send(StoreMsg {
                            sender: self_ref.clone(),
                            content: new_store_msg_content.clone(),
                        })
                        .await;
                    }
                    self.state = StoreState::Finalizing(0, result);
                }
            }
            (NodeMsgContent::FinalizationAck, StoreState::Finalizing(n_acks, result)) => {
                log::debug!("Got new ack");
                let new_n_acks = n_acks + 1;
                let new_state = if new_n_acks == self.n_nodes {
                    let mut x: Option<CompletedCallBack> = None;
                    mem::swap(&mut x, &mut self.transaction_completed_callback);

                    let dupa = x.unwrap()(result);
                    dupa.await;
                    StoreState::Init
                } else {
                    StoreState::Finalizing(new_n_acks, result)
                };
                self.state = new_state;
            }
            _ => {
                panic!("Unexpected message")
            }
        }
        return ();
    }
}

fn add(u: u64, i: i32) -> u64 {
    if i.is_negative() {
        let i2 = (-i) as u64;
        u - i2 as u64
    } else {
        u + i as u64
    }
}

#[async_trait::async_trait]
impl Handler<StoreMsg> for Node {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: StoreMsg) {
        if self.enabled {
            fn check_if_transaction(pending_transaction: &Option<Transaction>) {
                if pending_transaction.is_none() {
                    log::error!("There is no transaction at the moment!");
                }
            }

            match msg.content {
                StoreMsgContent::RequestVote(t) => {
                    log::debug!("Receiving vote request");
                    let b: bool = if t.shift > 0 {
                        true
                    } else {
                        let shift_u64 = (-t.shift) as u64;
                        self.products
                            .iter()
                            .filter(|x| x.pr_type == t.pr_type)
                            .all(|x| x.price > shift_u64)
                    };
                    let to_send = if b {
                        self.pending_transaction = Some(t);
                        TwoPhaseResult::Ok
                    } else {
                        TwoPhaseResult::Abort
                    };
                    msg.sender
                        .send(NodeMsg {
                            content: NodeMsgContent::RequestVoteResponse(to_send),
                        })
                        .await;
                }
                StoreMsgContent::Commit => {
                    check_if_transaction(&self.pending_transaction);
                    let t: Transaction = self.pending_transaction.unwrap();
                    self.products
                        .iter_mut()
                        .for_each(|x: &mut Product| x.price = add(x.price, t.shift));

                    msg.sender
                        .send(NodeMsg {
                            content: NodeMsgContent::FinalizationAck,
                        })
                        .await;
                }
                StoreMsgContent::Abort => {
                    check_if_transaction(&self.pending_transaction);
                    msg.sender
                        .send(NodeMsg {
                            content: NodeMsgContent::FinalizationAck,
                        })
                        .await;
                }
            }
        }
        return ();
    }
}

#[async_trait::async_trait]
impl Handler<ProductPriceQuery> for Node {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, msg: ProductPriceQuery) {
        if self.enabled {
            let product_price = self
                .products
                .iter()
                .find(|x| x.identifier == msg.product_ident)
                .map(|x| x.price);
            msg.result_sender
                .send(ProductPrice(product_price))
                .await
                .unwrap();
        }
    }
}

#[async_trait::async_trait]
impl Handler<Disable> for Node {
    async fn handle(&mut self, _self_ref: &ModuleRef<Self>, _msg: Disable) {
        self.enabled = false;
    }
}

#[async_trait::async_trait]
impl Handler<TransactionMessage> for CyberStore2047 {
    async fn handle(&mut self, self_ref: &ModuleRef<Self>, msg: TransactionMessage) {
        self.state = StoreState::Init;
        self.transaction_completed_callback = Some(msg.completed_callback);
        for node in self.nodes.iter() {
            node.send(StoreMsg {
                sender: self_ref.clone(),
                content: StoreMsgContent::RequestVote(msg.transaction.clone()),
            })
            .await;
        }
        self.state = StoreState::VoteCouting(0, false);
    }
}
