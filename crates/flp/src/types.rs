use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WalletDelegations {
    #[serde(rename = "walletFrom")]
    pub wallet_from: String,
    #[serde(rename = "walletTo")]
    pub wallet_to: String,
    pub factor: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SetBalancesData {
    pub eoa: String,
    pub amount: u128,
    pub ar_address: String,
}
