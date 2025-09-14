use codecrafters_kafka::response::{KafkaResponse, KafkaResponseHeader};

mod common;
use common::TestContext;

#[tokio::test]
async fn test_parse_correlation_id() {
    let mut ctx = TestContext::new().await;

    let request = KafkaResponse {
        message_size: 0,
        header: KafkaResponseHeader { correlation_id: 7 },
    };
    ctx.send_request(&request).await.unwrap();

    let response = ctx.parse_request().await.unwrap();

    assert_eq!(request, response);
}
