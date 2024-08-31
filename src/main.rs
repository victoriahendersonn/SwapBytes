mod app;
mod colors;
mod tabs;
mod theme;
mod opt;
mod network;

use clap::Parser;
use tokio::task::spawn;
use color_eyre::Result;
use futures::prelude::*;
use libp2p::multiaddr::Protocol;
use std::io::Write;

//use app::App;
use opt::{Opt, CliArgument};

pub use self::{
    colors::color_from_oklab,
    theme::THEME,
};

/** 
 * This is the main function that will run the application and 
 * present users with the initial screen of the application, asking
 * them to choose a username before proceeding with the application.
 */
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::parse();

    let (mut network_client, mut network_events, network_event_loop) =
        network::behaviour::swarm(opt.secret_key_seed).await?;

    // spwaning the network task for it to run in the background :)
    spawn(network_event_loop.run());
    
    // In case a listen address was provided use it, otherwise listen on any
    // address.
    match opt.listen_address {
        Some(addr) => network_client
            .start_listening(addr)
            .await
            .expect("Listening not to fail."),
        None => network_client
            .start_listening("/ip4/0.0.0.0/tcp/0".parse()?)
            .await
            .expect("Listening not to fail."),
    };

    // In case the user provided an address of a peer on the CLI, dial it.
    if let Some(addr) = opt.peer {
        let Some(Protocol::P2p(peer_id)) = addr.iter().last() else {
            return Err("Expect peer multiaddr to contain peer ID.".into());
        };
        network_client
            .dial(peer_id, addr)
            .await
            .expect("Dial to succeed");
    }

    match opt.argument {
        // Providing a file.
        CliArgument::Provide { path, name } => {
            // Advertise oneself as a provider of the file on the DHT.
            network_client.start_providing(name.clone()).await;

            loop {
                match network_events.next().await {
                    // Reply with the content of the file on incoming requests.
                    Some(network::Event::InboundRequest { request, channel }) => {
                        if request == name {
                            network_client
                                .respond_file(std::fs::read(&path)?, channel)
                                .await;
                        }
                    }
                    e => todo!("{:?}", e),
                }
            }
        }
        // Locating and getting a file.
        CliArgument::Get { name } => {
            // Locate all nodes providing the file.
            let providers = network_client.get_providers(name.clone()).await;
            if providers.is_empty() {
                return Err(format!("Could not find provider for file {name}.").into());
            }

            // Request the content of the file from each node.
            let requests = providers.into_iter().map(|p| {
                let mut network_client = network_client.clone();
                let name = name.clone();
                async move { network_client.request_file(p, name).await }.boxed()
            });

            // Await the requests, ignore the remaining once a single one succeeds.
            let file_content = futures::future::select_ok(requests)
                .await
                .map_err(|_| "None of the providers returned file.")?
                .0;

            std::io::stdout().write_all(&file_content)?;
        }
    }

    Ok(())

    // terminal UI 
    // color_eyre::install()?;
    // let terminal = ratatui::init();
    // let app_result = App::default().run(terminal);
    // ratatui::restore();
    
    // Ok(app_result?)
}