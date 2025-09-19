use tokio::fs;

use crate::{modules::metadata_log_file::payloads::MetadataLogFile, serde_kafka};

pub async fn load() {
    // let metadata_topic_file_path =
    //     "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
    let metadata_topic_file_path = "./log.log";
    let content = fs::read(metadata_topic_file_path).await.unwrap();

    let metadata: Vec<MetadataLogFile> = serde_kafka::from_bytes(&content).unwrap();

    tracing::debug!("{metadata:?}");
}

#[cfg(test)]
mod test {
    use uuid::Uuid;

    use crate::modules::metadata_log_file::payloads::{Record, RecordValue};

    use super::*;

    #[tokio::test]
    async fn test_parse() {
        load().await;
    }

    #[test]
    fn test_ser_de_empty() {
        let map: (MetadataLogFile, MetadataLogFile) = (
            MetadataLogFile {
                base_offset: 0,
                batch_length: 79,
                partition_leader_epoch: 1,
                magic_byte: 2,
                crc: -1335278212,
                attributes: 0,
                last_offset_delta: 0,
                base_timestamp: 1726045943832,
                max_timestamp: 1726045943832,
                producer_id: -1,
                producer_epoch: -1,
                base_sequence: -1,
                records: vec![Record {
                    length: 29,
                    attributes: 0,
                    timestamp_delta: 0,
                    offset_delta: 0,
                    keys: vec![],
                    value: RecordValue::FeatureLevelValue {
                        frame_version: 1,
                        value_type: 12,
                        version: 1,
                        name: "metadata-version".into(),
                        feature_level: 20,
                        tagged_fields_count: 0,
                    },
                    headers_array_count: 0,
                }],
            },
            MetadataLogFile {
                base_offset: 1,
                batch_length: 228,
                partition_leader_epoch: 1,
                magic_byte: 2,
                crc: 618336989,
                attributes: 0,
                last_offset_delta: 2,
                base_timestamp: 1726045957397,
                max_timestamp: 1726045957397,
                producer_id: -1,
                producer_epoch: -1,
                base_sequence: -1,
                records: vec![
                    Record {
                        length: 30,
                        attributes: 0,
                        timestamp_delta: 0,
                        offset_delta: 0,
                        keys: vec![],
                        value: RecordValue::TopicRecordValue {
                            frame_version: 1,
                            value_type: 2,
                            version: 0,
                            topic_name: "saz".into(),
                            topic_uuid: Uuid::from_bytes([
                                0x00u8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00,
                                0x00, 0x00, 0x00, 0x00, 0x91,
                            ]),
                            tagged_fields_count: 0,
                        },
                        headers_array_count: 0,
                    },
                    Record {
                        length: 72,
                        attributes: 0,
                        timestamp_delta: 0,
                        offset_delta: 1,
                        keys: vec![],
                        value: RecordValue::PartitionRecordValue {
                            frame_version: 1,
                            value_type: 3,
                            version: 1,
                            partition_id: 1,
                            topic_uuid: Uuid::from_bytes([
                                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00,
                                0x00, 0x00, 0x00, 0x00, 0x91,
                            ]),
                            replicas: vec![1],
                            in_sync_replicas: vec![1],
                            removing_replicas: vec![],
                            adding_replicas: vec![],
                            leader: 1,
                            leader_epoch: 0,
                            partition_epoch: 0,
                            directories: vec![Uuid::from_bytes([
                                0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00,
                                0x00, 0x00, 0x00, 0x00, 0x01,
                            ])],
                            tagged_fields_count: 0,
                        },
                        headers_array_count: 0,
                    },
                    Record {
                        length: 72,
                        attributes: 0,
                        timestamp_delta: 0,
                        offset_delta: 2,
                        keys: vec![],
                        value: RecordValue::PartitionRecordValue {
                            frame_version: 1,
                            value_type: 3,
                            version: 1,
                            partition_id: 1,
                            topic_uuid: Uuid::from_bytes([
                                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00,
                                0x00, 0x00, 0x00, 0x00, 0x91,
                            ]),
                            replicas: vec![1],
                            in_sync_replicas: vec![1],
                            removing_replicas: vec![],
                            adding_replicas: vec![],
                            leader: 1,
                            leader_epoch: 0,
                            partition_epoch: 0,
                            directories: vec![Uuid::from_bytes([
                                0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00,
                                0x00, 0x00, 0x00, 0x00, 0x01,
                            ])],
                            tagged_fields_count: 0,
                        },
                        headers_array_count: 0,
                    },
                ],
            },
        );

        let result: Vec<u8> = vec![
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x4f, 0x00, 0x00,
            0x00, 0x01, 0x02, 0xb0, 0x69, 0x45, 0x7c, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x01, 0x91, 0xe0, 0x5a, 0xf8, 0x18, 0x00, 0x00, 0x01, 0x91, 0xe0, 0x5a, 0xf8,
            0x18, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0x00, 0x00, 0x00, 0x01, 0x3a, 0x00, 0x00, 0x00, 0x01, 0x2e, 0x01, 0x0c, 0x00,
            0x11, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x2e, 0x76, 0x65, 0x72, 0x73,
            0x69, 0x6f, 0x6e, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x01, 0x00, 0x00, 0x00, 0xe4, 0x00, 0x00, 0x00, 0x01, 0x02, 0x24, 0xdb, 0x12, 0xdd,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x01, 0x91, 0xe0, 0x5b, 0x2d, 0x15,
            0x00, 0x00, 0x01, 0x91, 0xe0, 0x5b, 0x2d, 0x15, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
            0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x03, 0x3c, 0x00,
            0x00, 0x00, 0x01, 0x30, 0x01, 0x02, 0x00, 0x04, 0x73, 0x61, 0x7a, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x91, 0x00,
            0x00, 0x90, 0x01, 0x00, 0x00, 0x02, 0x01, 0x82, 0x01, 0x01, 0x03, 0x01, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x91, 0x02, 0x00, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x01,
            0x01, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x02, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x01, 0x00, 0x00, 0x90, 0x01, 0x00, 0x00, 0x04, 0x01, 0x82, 0x01, 0x01,
            0x03, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00,
            0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x91, 0x02, 0x00, 0x00, 0x00, 0x01, 0x02,
            0x00, 0x00, 0x00, 0x01, 0x01, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x02, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00, 0x80,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
        ];

        let wat = serde_kafka::to_bytes_mut(&map).unwrap();
        assert_eq!(result, wat.to_vec())
    }
}
