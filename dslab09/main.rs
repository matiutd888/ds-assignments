mod public_test;
mod solution;

use crate::solution::{
    CyberStore2047, Node, Product, ProductPrice, ProductPriceQuery, ProductType, Transaction,
    TransactionMessage, TwoPhaseResult,
};
use async_channel::{bounded, unbounded};
use executor::{ModuleRef, System};
use std::time::Duration;
use uuid::Uuid;

async fn send_query(node: &ModuleRef<Node>, product_ident: Uuid) -> ProductPrice {
    let (result_sender, result_receiver) = bounded::<ProductPrice>(1);
    node.send(ProductPriceQuery {
        product_ident,
        result_sender,
    })
    .await;
    result_receiver.recv().await.unwrap()
}

#[tokio::main]
async fn main() {
    // Initialize the system and the store:
    let mut system = System::new().await;
    let (transaction_done_tx, transaction_done_rx) = unbounded();
    let laptop_id = Uuid::new_v4();
    let book_id = Uuid::new_v4();
    let initial_laptop_price = 150000;
    let book_price = 5000;
    let products = vec![
        Product {
            identifier: laptop_id,
            pr_type: ProductType::Electronics,
            price: initial_laptop_price,
        },
        Product {
            identifier: book_id,
            pr_type: ProductType::Books,
            price: book_price,
        },
        Product {
            identifier: Uuid::new_v4(),
            pr_type: ProductType::Toys,
            price: 1000,
        },
    ];
    let node = system.register_module(Node::new(products)).await;
    let cyber_store = system
        .register_module(CyberStore2047::new(vec![node.clone()]))
        .await;

    // Increase prices of electronics:
    let electronics_price_shift = 100;
    let tx2 = transaction_done_tx.clone();
    cyber_store
        .send(TransactionMessage {
            transaction: Transaction {
                pr_type: ProductType::Electronics,
                shift: electronics_price_shift,
            },
            completed_callback: Box::new(|result| {
                Box::pin(async move {
                    tx2.send(result).await.unwrap();
                })
            }),
        })
        .await;
    assert_eq!(Ok(TwoPhaseResult::Ok), transaction_done_rx.recv().await);

    let tx3 = transaction_done_tx.clone();
    cyber_store
        .send(TransactionMessage {
            transaction: Transaction {
                pr_type: ProductType::Books,
                shift: -(book_price as i32),
            },
            completed_callback: Box::new(|result| {
                Box::pin(async move {
                    tx3.send(result).await.unwrap();
                })
            }),
        })
        .await;

    assert_eq!(Ok(TwoPhaseResult::Abort), transaction_done_rx.recv().await);
    // Check the new price of the latop:
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        initial_laptop_price + (electronics_price_shift as u64),
        send_query(&node, laptop_id).await.0.unwrap()
    );
    assert_eq!(book_price, send_query(&node, book_id).await.0.unwrap());

    println!("System can execute a simple transaction!");
    system.shutdown().await;
}
