use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use clap::{arg, Parser};
use dogstatsd::Client;
use futures_util::future::join_all;
use futures_util::stream::StreamExt;
use mysql_async::prelude::*;
use mysql_async::{Pool, SslOpts, TxOpts};
use sp_core::crypto;
use sp_core::crypto::Ss58Codec;
use subxt::client::OnlineClientT;
use subxt::utils::AccountId32;
use subxt::{Config, OnlineClient, PolkadotConfig};
use tokio::sync::Semaphore;

use crate::polkadot::para_inclusion::events::CandidateBacked;

#[subxt::subxt(runtime_metadata_path = "../subxt_metadata/polkadot_runtime.scale")]
pub mod polkadot {}

#[derive(Parser)]
struct CommandLine {
    /// Sets a polkadot node url
    #[arg(short = 'p', default_value = "wss://rpc.polkadot.io:443", env)]
    polkadot_url: String,

    /// Sets a mysql url
    #[arg(
        short = 'm',
        default_value = "mysql://user:password@localhost:3306/state",
        env
    )]
    mysql_url: String,

    /// Sets if mysql connection require ssl
    #[arg(short = 's', default_value = "false", env)]
    mysql_ssl: bool,

    /// Default numbers of past blocks to fetch at start,
    #[arg(short = 'n', default_value = "30000", env)] // 15*24*60*10
    number_of_past_blocks: u32,
}

fn to_ss58check(account: &AccountId32) -> String {
    crypto::AccountId32::new(*account.as_ref()).to_ss58check()
}

#[tokio::main()]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = CommandLine::parse();

    let metrics = dogstatsd::Client::new(dogstatsd::Options::default()).unwrap();

    let dbpool = connect_mysql(&args.mysql_url, None)?;
    let client = connect_polkadot(&args.polkadot_url).await?;

    let mut sub = client.blocks().subscribe_finalized().await?;

    let current_block = sub.next().await.unwrap()?;

    let starting_block = current_block
        .number()
        .saturating_sub(args.number_of_past_blocks);

    let present_blocks = "SELECT UNIQUE(block) FROM backing WHERE block >= ?"
        .with((starting_block,))
        .run(&dbpool)
        .await?
        .collect::<u32>()
        .await?;

    println!("Started catching up");
    let mut handles = vec![];

    // Process previously missed blocks
    let permit = Arc::new(Semaphore::new(200));
    for n in (starting_block..=current_block.number()).filter(|n| !present_blocks.contains(n)) {
        let client = client.clone();
        let dbpool = dbpool.clone();
        let permit = permit.clone();
        handles.push(tokio::spawn(async move {
            let _allow = permit.acquire_owned().await.unwrap();
            let backing_result = fetch_backing_info(client, n).await;
            commit_to_db(dbpool, &backing_result).await;
            print_backing_result(n, &backing_result);
        }));
    }

    join_all(handles).await;
    println!("Listening for new blocks");

    // Receive realtime blocks and process
    while let Some(block) = sub.next().await {
        let n = block.unwrap().number();
        let client = client.clone();
        let dbpool = dbpool.clone();
        let backing_result = fetch_backing_info(client, n).await;
        commit_to_db(dbpool, &backing_result).await;
        print_backing_result(n, &backing_result);
        report_metrics(&metrics, &backing_result);
    }

    Ok(())
}

fn report_metrics(metrics: &Client, backing_result: &[(u32, String, bool)]) {
    let mut error = None;
    for (_, ss58, hit) in backing_result {
        let tags = format!("ss58:{ss58}");
        let name = format!(
            "polka.validating.backing.{}",
            if *hit { "hit" } else { "miss" }
        );
        if let Err(e) = metrics.incr(name, &[tags]) {
            error = error.or(Some(e));
        }
    }
    if error.is_some() {
        println!("{}", error.unwrap());
    }
}

fn print_backing_result(n: u32, backing_result: &[(u32, String, bool)]) {
    let yes = backing_result.iter().filter(|&x| x.2).count();
    let no = backing_result.iter().filter(|&x| !x.2).count();
    println!("{}: hit {} miss {}", n, yes, no);
}

async fn commit_to_db(dbpool: Pool, backing_result: &Vec<(u32, String, bool)>) {
    let mut tx = dbpool.start_transaction(TxOpts::default()).await.unwrap();
    tx.exec_batch(
        "INSERT INTO backing (block, ss58, backed) VALUES (?, ?, ?);",
        backing_result,
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

async fn fetch_backing_info<T, Client>(client: Client, number: u32) -> Vec<(u32, String, bool)>
where
    Client: OnlineClientT<T>,
    T: Config,
{
    let this_block_hash = client
        .rpc()
        .block_hash(Some(number.into()))
        .await
        .unwrap()
        .unwrap();

    let this_block_storage = client.storage().at(Some(this_block_hash)).await.unwrap();

    let prev_block_storage = {
        let hash = client
            .rpc()
            .block_hash(Some(number.saturating_sub(1).into()))
            .await
            .unwrap()
            .unwrap();

        client.storage().at(Some(hash)).await.unwrap()
    };

    let on_chain_votes = this_block_storage
        .fetch(&polkadot::storage().para_inherent().on_chain_votes())
        .await
        .unwrap()
        .unwrap();

    let para_backers: HashMap<u32, Vec<u32>> = on_chain_votes
        .backing_validators_per_candidate
        .into_iter()
        .map(|(c, v)| {
            (
                c.descriptor.para_id.0,
                v.into_iter().map(|(idx, _)| idx.0).collect(),
            )
        })
        .collect();

    let session_validators = prev_block_storage
        .fetch(&polkadot::storage().session().validators())
        .await
        .unwrap()
        .unwrap();

    let active_validator_ix = prev_block_storage
        .fetch(
            &polkadot::storage()
                .paras_shared()
                .active_validator_indices(),
        )
        .await
        .unwrap()
        .unwrap();

    let validator_groups = prev_block_storage
        .fetch(&polkadot::storage().para_scheduler().validator_groups())
        .await
        .unwrap()
        .unwrap();

    let mut backing_result = vec![];
    let events = client.events().at(Some(this_block_hash)).await.unwrap();

    for evt in events.find::<CandidateBacked>() {
        let CandidateBacked(receipt, _, _, group_id) = evt.unwrap();

        let actual_voters = &para_backers[&receipt.descriptor.para_id.0];
        let expected_voters = &validator_groups[group_id.0 as usize];

        for voter in expected_voters {
            let global_validator_ix = active_validator_ix[voter.0 as usize].0 as usize;
            let account_id = &session_validators[global_validator_ix];
            let backed = actual_voters.contains(&voter.0);
            backing_result.push((number, to_ss58check(account_id), backed));
        }
    }

    backing_result
}

async fn connect_polkadot(url: &str) -> Result<OnlineClient<PolkadotConfig>, subxt::error::Error> {
    let client = OnlineClient::<PolkadotConfig>::from_url(url).await.unwrap();

    // setup proper ss58 config
    crypto::set_default_ss58_version(
        client
            .constants()
            .at(&polkadot::constants().system().ss58_prefix())?
            .into(),
    );

    // setup runtime updater
    let update_client = client.updater();
    tokio::spawn(async move {
        while let Err(e) = update_client.perform_runtime_updates().await {
            println!("Runtime update failed with result={e:?}");
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });

    Ok(client)
}

fn connect_mysql(url: &str, sslopts: Option<SslOpts>) -> Result<Pool, mysql_async::UrlError> {
    use mysql_async::{Opts, OptsBuilder};

    let mut builder = OptsBuilder::from_opts(Opts::from_url(url)?);
    if let Some(sslopts) = sslopts {
        builder = builder.ssl_opts(sslopts);
    }

    Ok(Pool::new(builder))
}
