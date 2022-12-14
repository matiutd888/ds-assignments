#[cfg(test)]
mod tests {
    use async_channel::unbounded;
    use ntest::timeout;

    use crate::solution::{
        CyberStore2047, Node, Product, ProductType, Transaction, TransactionMessage, TwoPhaseResult,
    };
    use executor::System;
    use uuid::Uuid;

    #[tokio::test]
    #[timeout(300)]
    async fn overflow() {
        // Given:
        let mut system = System::new().await;
        let (transaction_done_tx, transaction_done_rx) = unbounded();
        let products = vec![Product {
            identifier: Uuid::new_v4(),
            pr_type: ProductType::Electronics,
            price: u64::MAX,
        }];
        let node0 = system.register_module(Node::new(products.clone())).await;
        let node1 = system.register_module(Node::new(products)).await;
        let cyber_store =
            system.register_module(CyberStore2047::new(vec![node0, node1])).await;

        // When:
        cyber_store
            .send(TransactionMessage {
                transaction: Transaction {
                    pr_type: ProductType::Electronics,
                    shift: 1,
                },
                completed_callback: Box::new(|result| {
                    Box::pin(async move {
                        transaction_done_tx.send(result).await.unwrap();
                    })
                }),
            })
            .await;

        // Then:
        assert_eq!(
            TwoPhaseResult::Abort,
            transaction_done_rx.recv().await.unwrap()
        );
        system.shutdown().await;
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn overflow2() {
        println!("overflow 2");
        // Given:
        let mut system = System::new().await;
        let (transaction_done_tx, transaction_done_rx) = unbounded();
        let products = vec![Product {
            identifier: Uuid::new_v4(),
            pr_type: ProductType::Electronics,
            price: u64::MAX,
        }];
        let node0 = system.register_module(Node::new(products.clone())).await;
        let node1 = system.register_module(Node::new(products)).await;
        let cyber_store =
            system.register_module(CyberStore2047::new(vec![node0, node1])).await;

        // When:
        cyber_store
            .send(TransactionMessage {
                transaction: Transaction {
                    pr_type: ProductType::Electronics,
                    shift: i32::MIN,
                },
                completed_callback: Box::new(|result| {
                    Box::pin(async move {
                        transaction_done_tx.send(result).await.unwrap();
                    })
                }),
            })
            .await;

        // Then:
        assert_eq!(
            TwoPhaseResult::Ok,
            transaction_done_rx.recv().await.unwrap()
        );
        system.shutdown().await;
    }

    #[tokio::test]
    #[timeout(300)]
    async fn transaction_with_two_nodes_completes() {
        // Given:
        let mut system = System::new().await;
        let (transaction_done_tx, transaction_done_rx) = unbounded();
        let products = vec![Product {
            identifier: Uuid::new_v4(),
            pr_type: ProductType::Electronics,
            price: 180,
        }];
        let node0 = system.register_module(Node::new(products.clone())).await;
        let node1 = system.register_module(Node::new(products)).await;
        let cyber_store = system
            .register_module(CyberStore2047::new(vec![node0, node1]))
            .await;

        // When:
        cyber_store
            .send(TransactionMessage {
                transaction: Transaction {
                    pr_type: ProductType::Electronics,
                    shift: -50,
                },
                completed_callback: Box::new(|result| {
                    Box::pin(async move {
                        transaction_done_tx.send(result).await.unwrap();
                    })
                }),
            })
            .await;

        // Then:
        assert_eq!(
            TwoPhaseResult::Ok,
            transaction_done_rx.recv().await.unwrap()
        );
        system.shutdown().await;
    }

    #[tokio::test]
    #[timeout(300)]
    async fn forbidden_transaction_with_two_nodes_is_aborted() {
        // Given:
        let cheap_product = Product {
            identifier: Uuid::new_v4(),
            pr_type: ProductType::Electronics,
            price: 1,
        };

        let mut system = System::new().await;
        let (transaction_done_tx, transaction_done_rx) = unbounded();
        let products1 = vec![Product {
            identifier: Uuid::new_v4(),
            pr_type: ProductType::Electronics,
            price: 180,
        }];
        let products0 = vec![cheap_product];
        let node0 = system.register_module(Node::new(products0)).await;
        let node1 = system.register_module(Node::new(products1)).await;
        let cyber_store = system.register_module(
            CyberStore2047::new(vec![node0.clone(), node1.clone()]),
        )
        .await;

        // When:
        cyber_store
            .send(TransactionMessage {
                transaction: Transaction {
                    pr_type: ProductType::Electronics,
                    shift: -50,
                },
                completed_callback: Box::new(|result| {
                    Box::pin(async move {
                        transaction_done_tx.send(result).await.unwrap();
                    })
                }),
            })
            .await;


        assert_eq!(
            TwoPhaseResult::Abort,
            transaction_done_rx.recv().await.unwrap()
        );
        system.shutdown().await;
    }

}
