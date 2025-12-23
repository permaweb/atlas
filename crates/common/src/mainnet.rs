/// common utils for retrieving, filtering and sorting
/// ao mainnet network data (ao.N.1 messages) extracted from 
/// Arweave blocks using GQL gateways.
/// 
/// the mainnet have 2 type of tags for messages, in Atlas,
/// we label them as type A and type B:
/// 
/// - type A follows lower-case tags key format
/// - type B follows Header-Case tags key format
/// - type A start blockheight: 1_594_020 -- Jan 22 2025
/// - type B start blockheight: 1_616_999 --  Feb 25 2025

use crate::constants::{DATA_PROTOCOL_A_START, DATA_PROTOCOL_B_START};
use serde::{Serialize, Deserialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Tag {
    pub key: String,
    pub value: String
}

impl Tag {
    pub fn from_kv(key: &str, value: &str) -> Self {
        Self { key: key.to_string(), value: value.to_string() }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MainnetBlockMessagesMeta {
    pub tx_id: String,
    pub owner: String,
    pub block_height: u32,
    pub block_timestamp: u64,
    pub tags: Vec<Tag>
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MainnetBlockMessagesPage {
    pub mappings: Vec<MainnetBlockMessagesMeta>,
    pub has_next_page: bool,
    pub end_cursor: Option<String>,
}