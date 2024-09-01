mod app;
mod colors;
mod tabs;
mod theme;
mod network;

use std::path::PathBuf;

use clap::Parser;
use futures::StreamExt;
use libp2p::{gossipsub, multiaddr::Protocol, swarm::behaviour, Multiaddr, PeerId};
use network::Client;
use tokio::{io::{self, AsyncBufReadExt}, select, task::spawn};

use color_eyre::Result;

//use app::App;
use crate::network::behaviour::swarm;

pub use self::{
    colors::color_from_oklab,
    theme::THEME,
};


// /**
//  * Contains all neccesary information regarding the SwapBytes application.
//  */
// #[derive(Debug, Default)]
//  pub struct SwapBytes {
//     pub users: Vec<String>,
//     pub current_user: Option<String>,
//     pub rooms: Vec<String>,
//     pub current: String,
// }

// impl SwapBytes {
//     pub fn new() -> Self {
//         Self {
//             users: vec![],
//             current_user: None,
//             rooms: vec!["global-chat".to_string()],
//             current: "global-chat".to_string(),
//         }
//     }
// }

/** 
 * This is the main function that will run the application and 
 * present users with the initial screen of the application, asking
 * them to choose a username before proceeding with the application.
 */
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    swarm().await
    // let (mut client, mut event_receiver, event_loop) = swarm().await?;

    // // start event loop
    // spawn(event_loop.run());

    // let swap_bytes = SwapBytes::new();
    // let mut stdin = io::BufReader::new(io::stdin()).lines();

    // println!("Enter chat messages one line at a time:");

    // loop {
    //     select! {
    //         Ok(Some(line)) = stdin.next_line() => {
    //             if line.starts_with("/") {
    //                 handle_command(&mut client, &line).await;
    //             } else {
    //                 client.send_message(swap_bytes.current.clone(), line).await;
    //             }   
    //         }
    //         // event = event_receiver.select_next_some() => match event {
                
    //         // }
    //     }
    // }

    // terminal UI 
    // color_eyre::install()?;
    // let terminal = ratatui::init();
    // let app_result = App::default().run(terminal);
    // ratatui::restore();
    
    // Ok(app_result?)
}


// async fn handle_command(client: &mut Client, input: &str) {
//     let parts: Vec<&str> = input.split_whitespace().collect();
//     match parts[0] {
//         "/create" => {
//             if parts.len() < 2 {
//                 println!("Usage: /create <room_name>");
//             } else {
//                 let room_name = parts[1].to_string();
//                 client.create_room(room_name).await;
//                 println!("Room created");
//             }
//         }
//         // "/join" => {
//         //     if parts.len() < 2 {
//         //         println!("Usage: /join <room_name>");
//         //     } else {
//         //         let room_name = parts[1].to_string();
//         //         client.join_room(room_name);
//         //         println!("Joined room");
//         //     }
//         // }
//         // "/request" => {
//         //     if parts.len() < 3 {
//         //         println!("Usage: /request <file_name> <peer_id>");
//         //     } else {
//         //         let file_name = parts[1].to_string();
//         //         let peer_id = parts[2].to_string();
//         //         client.request_file(peer_id, file_name);
//         //         println!("File request sent");
//         //     }
//         // }
//         // "/dm" => {
//         //     if parts.len() < 3 {
//         //         println!("Usage: /dm <peer_id> <message>");
//         //     } else {
//         //         let peer_id = parts[1].to_string();
//         //         let message = parts[2..].join(" ");
//         //         client.direct_message(, );
//         //         println!("Direct message sent");
//         //     }
//         // }
//         "/rooms" => {
//             println!("Current rooms:");
//             client.get_rooms().await;
//         }
//         // "/nickname" =>{
//         //     let local_peer_id_record = kad::RecordKey::new(&self_peer_id.to_string());
//         //     let record = kad::Record {
//         //         key: local_peer_id_record,
//         //         value: args[1].as_bytes().to_vec(),
//         //         publisher: None,
//         //         expires: None,
//         //     };
//         //     kademlia
//         //         .put_record(record, kad::Quorum::One)
//         //         .expect("Failed to store record locally.");
//         // }
//         _ => {
//             println!("Unknown command");
//         }
//     }
// }