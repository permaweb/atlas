use crate::constants::{
    AO_AUTHORITY, ARWEAVE_GATEWAY, DAI_ORACLE_PID, STETH_ORACLE_PID, USDS_ORACLE_PID,
};
use anyhow::{Error, anyhow};
use serde_json::{Value, json};

#[derive(PartialEq, Clone, Copy, Debug)]
pub enum Oracle {
    USDS,
    DAI,
    STETH,
    All,
    Unknown,
}

impl Oracle {
    pub fn resolve(&self) -> String {
        match self {
            &Oracle::USDS => format!("[\"{USDS_ORACLE_PID}\"]"),
            &Oracle::DAI => format!("[\"{DAI_ORACLE_PID}\"]"),
            &Oracle::STETH => format!("[\"{STETH_ORACLE_PID}\"]"),
            &Oracle::All => {
                format!("[\"{USDS_ORACLE_PID}\", \"{DAI_ORACLE_PID}\", \"{STETH_ORACLE_PID}\"]")
            }
            &Oracle::Unknown => String::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OracleStakers {
    pub oracle: Oracle,
    query: Option<Value>,
    server_resp: Option<Value>,
    id: Option<Vec<String>>,
}

impl OracleStakers {
    pub fn new(oracle: &str) -> Self {
        match oracle.to_ascii_lowercase().as_str() {
            "usds" => OracleStakers {
                oracle: Oracle::USDS,
                query: None,
                server_resp: None,
                id: None,
            },
            "dai" => OracleStakers {
                oracle: Oracle::DAI,
                query: None,
                server_resp: None,
                id: None,
            },
            "steth" => OracleStakers {
                oracle: Oracle::STETH,
                query: None,
                server_resp: None,
                id: None,
            },
            "all" => OracleStakers {
                oracle: Oracle::All,
                query: None,
                server_resp: None,
                id: None,
            },
            _ => OracleStakers {
                oracle: Oracle::Unknown,
                query: None,
                server_resp: None,
                id: None,
            },
        }
    }

    pub fn build(&mut self) -> Result<&mut Self, Error> {
        if self.oracle == Oracle::Unknown {
            return Err(anyhow!("error: unknown oracle type"));
        };

        let first_var = if self.oracle != Oracle::All { 1 } else { 3 };

        let template = r#"
            query GetDetailedTransactions {
    transactions(
        first: $firstvar
        sort: HEIGHT_DESC
        owners: ["$ownervar"]
        tags: [
        { name: "Action", values: ["Set-Balances"] },
        {name: "From-Process", values: $oraclevar }
        ]
    ) {
        edges {
        cursor
        node {
            id
            owner {
            address
            }
            tags {
            name
            value
            }
            block {
            id
            height
            }
        }
        }
        pageInfo {
        hasNextPage
        }
    }
    }
        "#;

        // formatting as arweave.net doesnt support dynamic vars on server level
        let query = template
            .replace("$firstvar", &first_var.to_string())
            .replace("$ownervar", AO_AUTHORITY)
            .replace("$oraclevar", &self.oracle.resolve());

        println!("QUERY: {:?}", query);

        let vars = json!({
            "owner": AO_AUTHORITY,
            "oracle": self.oracle.resolve()
        });

        let body = json!({
            "query": query,
            "variables": vars // ignored on server level but kept for future compatibility with other gateways
        });

        self.query = Some(body);

        Ok(self)
    }

    pub fn send(&mut self) -> Result<&mut Self, Error> {
        let url = format!("{ARWEAVE_GATEWAY}/graphql");
        let req = ureq::post(url)
            .send_json(self.query.clone())?
            .body_mut()
            .read_to_string()?;
        let res: Value = serde_json::from_str(&req)?;
        self.server_resp = Some(res);
        Ok(self)
    }
    pub fn id(&mut self) -> Result<Vec<String>, Error> {
        if self.id.is_none() {
            self.set_id()?;
        }

        self.id
            .clone()
            .ok_or(anyhow!("error while retrieving the message id"))
    }
    fn set_id(&mut self) -> Result<Vec<String>, Error> {
        if self.id.is_some() {
            return Err(anyhow!("error: message id is already set"));
        };
        let res = self.server_resp.clone().ok_or(anyhow!(
            "error: no gql server response was made successfully"
        ))?;
        let edges = res
            .get("data")
            .and_then(|v| v.get("transactions"))
            .and_then(|v| v.get("edges"))
            .and_then(|v| v.as_array())
            .ok_or(anyhow!(
                "error: no ao message edges found for the given query"
            ))?;

        let ids: Vec<String> = edges
            .iter()
            .filter_map(|edge| {
                edge.get("node")
                    .and_then(|node| node.get("id"))
                    .and_then(|id| id.as_str())
                    .map(|s| s.to_string())
            })
            .collect();

        if ids.is_empty() {
            return Err(anyhow!("error: no ao message id found for the given query"));
        }

        self.id = Some(ids.clone());
        Ok(ids)
    }
}

#[cfg(test)]
mod test {
    use crate::gql::OracleStakers;
    #[test]
    fn test_single_oracle_usds_stakers() {
        let mut oracle = OracleStakers::new("steth");
        let _res = oracle.build().unwrap().send().unwrap();
        let id = oracle.id().unwrap();
        println!("ORACLE ID: {:?}", id);
        assert_eq!(id.len(), 1);
    }
    #[test]
    fn test_all_oracle_stakers() {
        let mut oracle = OracleStakers::new("all");
        let _res = oracle.build().unwrap().send().unwrap();
        // noticied arweave gql gateway behavior is returning IDs in this order:
        // USDS / STETH / DAI
        let id = oracle.id().unwrap();
        println!("ORACLE IDS: {:?}", id);
        assert_eq!(id.len(), 3);
    }
}
