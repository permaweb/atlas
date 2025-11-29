use std::sync::Arc;

use anyhow::Result;
use chrono::Utc;
use common::gql::OracleStakers;
use flp::{
    set_balances::parse_flp_balances_setting_res,
    types::{DelegationsRes, SetBalancesData},
    wallet::get_wallet_delegations,
};
use futures::{stream, StreamExt};
use serde_json::to_string;

use crate::{
    clickhouse::{
        Clickhouse, FlpPositionRow, OracleSnapshotRow, WalletBalanceRow, WalletDelegationRow,
    },
    config::Config,
};

pub struct Indexer {
    config: Config,
    clickhouse: Clickhouse,
}

impl Indexer {
    pub fn new(config: Config, clickhouse: Clickhouse) -> Self {
        Indexer { config, clickhouse }
    }

    pub async fn run(&self) -> Result<()> {
        self.clickhouse.ensure().await?;
        self.run_once().await?;
        let mut interval = tokio::time::interval(self.config.interval);
        loop {
            interval.tick().await;
            if let Err(err) = self.run_once().await {
                eprintln!("index cycle error: {err:?}");
            }
        }
    }

    async fn run_once(&self) -> Result<()> {
        for ticker in &self.config.tickers {
            self.index_ticker(ticker).await?;
        }
        Ok(())
    }

    async fn index_ticker(&self, ticker: &str) -> Result<()> {
        let now = Utc::now();
        let ticker_owned = ticker.to_string();
        let (tx_id, balances) = load_balances(ticker_owned.clone()).await?;
        self.clickhouse
            .insert_oracles(&[OracleSnapshotRow {
                ts: now,
                ticker: ticker_owned.clone(),
                tx_id: tx_id.clone(),
            }])
            .await?;

        let pairs: Vec<(SetBalancesData, DelegationsRes)> = stream::iter(
            balances
                .into_iter()
                .map(|entry| async move { (entry.clone(), load_delegations(entry.ar_address).await) }),
        )
        .buffer_unordered(self.config.concurrency)
        .collect()
        .await;

        let mut balance_rows = Vec::with_capacity(pairs.len());
        let mut delegation_rows = Vec::with_capacity(pairs.len());
        let mut position_rows = Vec::new();

        for (entry, delegation) in pairs {
            balance_rows.push(WalletBalanceRow {
                ts: now,
                ticker: ticker_owned.clone(),
                wallet: entry.ar_address.clone(),
                amount: entry.amount.clone(),
                tx_id: tx_id.clone(),
            });
            delegation_rows.push(WalletDelegationRow {
                ts: now,
                wallet: entry.ar_address.clone(),
                payload: to_string(&delegation)?,
            });
            for pref in delegation.delegation_prefs {
                position_rows.push(FlpPositionRow {
                    ts: now,
                    ticker: ticker_owned.clone(),
                    wallet: entry.ar_address.clone(),
                    project: pref.wallet_to,
                    factor: pref.factor,
                    amount: entry.amount.clone(),
                });
            }
        }

        self.clickhouse.insert_balances(&balance_rows).await?;
        self.clickhouse.insert_delegations(&delegation_rows).await?;
        self.clickhouse.insert_positions(&position_rows).await?;
        Ok(())
    }
}

async fn load_balances(ticker: String) -> Result<(String, Vec<SetBalancesData>)> {
    tokio::task::spawn_blocking(move || {
        let oracle = OracleStakers::new(&ticker).build()?.send()?;
        let tx_id = oracle.clone().last_update()?;
        let data = parse_flp_balances_setting_res(&tx_id)?;
        Ok((tx_id, data))
    })
    .await?
}

async fn load_delegations(address: String) -> DelegationsRes {
    let fallback = Arc::new(address);
    match tokio::task::spawn_blocking({
        let address = fallback.clone();
        move || get_wallet_delegations(&address)
    })
    .await
    {
        Ok(Ok(data)) => data,
        _ => DelegationsRes::pi_default(fallback),
    }
}
