use crate::types::WalletDelegations;
use anyhow::Error;
use common::gateway::download_tx_data;
use common::gql::get_user_delegation;

pub fn get_wallet_delegations(address: &str) -> Result<WalletDelegations, Error> {
    let delegation_txid = get_user_delegation(address)?;
    let delegation_data = download_tx_data(&delegation_txid)?;
    let res: WalletDelegations = serde_json::from_slice(&delegation_data)?;
    Ok(res)
}

#[cfg(test)]
mod tests {
    use crate::wallet::get_wallet_delegations;

    #[test]
    fn get_wallet_delegations_test() {
        let address = "vZY2XY1RD9HIfWi8ift-1_DnHLDadZMWrufSh-_rKF0";
        let req = get_wallet_delegations(address).unwrap();
        println!("wallet delegations: {:?}", req);
        assert!(req.factor != 0);
    }
}
