use clap::Parser;
use futures::SinkExt;
use libp2p::Multiaddr;

use serde::{Deserialize, Serialize};
use SwapBytes::network;
use SwapBytes::state::STATE;

use std::error::Error;
use std::path::PathBuf;
use network::behaviour;
use network::command::Command;

use tokio::{
    io::{self, AsyncBufReadExt},
    select, spawn,
};

use color_eyre::Result;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectMessageRequest(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectMessageResponse(pub String);

/**
 * Main function that will run the application and handle the user's input accordingly.
 */
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // swarm
    let (mut client, mut event_receiver, event_loop) = behaviour::swarm().await?;

    // start event loop
    spawn(event_loop.run());

    let mut stdin: io::Lines<io::BufReader<io::Stdin>> = io::BufReader::new(io::stdin()).lines();

    loop {
        select! {
            Ok(Some(line)) = stdin.next_line() => {
                if line.is_empty() {
                    println!("Please enter something!");
                    continue;
                }

                let mut command = Command::Unknown;

                if line.starts_with("/") {
                    // it'll be a command, so need to create the command accodingly
                    command = create_command(&line).await;
                } else {
                    // if not a command, then it's a message to the chat that they're in...
                    // so create a message!
                    let state = STATE.lock().unwrap();
                    let message = format!("{}", line);

                    // turn the message into a command as that makes it easier to send to the client
                    command = Command::Message {
                        room: state.current_room.clone(),
                        message: message.clone(),
                    }
                }

                client.sender.send(command).await?;
            }
        }   
    }

    // color_eyre::install()?;
    // let mut terminal: ratatui::Terminal<ratatui::prelude::CrosstermBackend<std::io::Stdout>> = ratatui::init();
    // let mut app_result = App::new();

    // loop {
    //     terminal.draw(|frame| app_result.draw(frame))?;

    //     if let Event::Key(key) = event::read()? {
    //         match app_result.input_mode {
    //             InputMode::Normal => match key.code {
    //                 KeyCode::Char('e') => {
    //                     app_result.input_mode = InputMode::Editing;
    //                 }
    //                 KeyCode::Char('q') => {
    //                     return Ok(());
    //                 }
    //                 _ => {}
    //             },
    //             InputMode::Editing if key.kind == KeyEventKind::Press => match key.code {
    //                 KeyCode::Enter => app_result.submit_message(),
    //                 KeyCode::Char(to_insert) => app_result.enter_char(to_insert),
    //                 KeyCode::Backspace => app_result.delete_char(),
    //                 KeyCode::Left => app_result.move_cursor_left(),
    //                 KeyCode::Right => app_result.move_cursor_right(),
    //                 KeyCode::Esc => app_result.input_mode = InputMode::Normal,
    //                 _ => {}
    //             },
    //             InputMode::Editing => {}
    //         }
    //     }
    // }

    // ratatui::restore();
}


/**
 * Creates a command based on the user's input.
 */
async fn create_command(input: &str) -> Command {
    let user_input: Vec<&str> = input.split_whitespace().collect();

    match user_input[0] {
        "/dm" => {
            if user_input.len() < 3 {
                println!("Usage: /dm <peer_nickname> <message>");
                Command::Error
            } else {
                Command::DirectMessage {
                    peer_nickname: user_input[1].to_string(),
                    message: user_input[2..].join(" "),
                }
            }
        }
        "/trade" => {
            if user_input.len() < 2 || user_input.len() > 3 {
                Command::Error
            } else {
                Command::TradeRequest {
                    peer_nickname: user_input[1].to_string(),
                    message: Some(user_input[2..].join(" ")),
                }
            }
        }
        "/list-files" => Command::ListFiles,
        "/list-peers" => {
            if (user_input.len() < 1) || (user_input.len() > 1) {
                println!("Usage: /list-peers");
                Command::Error
            } else {
                Command::ListPeers
            }
        }
        "/set-nickname" => {
            if (user_input.len() < 2) || (user_input.len() > 2) {
                println!("Usage: /set-nickname <new_nickname>");
                Command::Error
            } else {
                Command::SetNickname {
                    new_nickname: (user_input[1].to_string()),
                }
            }
        }
        "/post-offer" => {
            if user_input.len() < 2 || (user_input.len() > 3) {
                println!("Usage: /post-offer <file_name>");
                Command::Error
            } else {
                // Command::StartProviding {
                //     file_name: user_input[1].to_string()
                // }
                Command::Error
            }
        },
        //"/request-file" => Command::RequestFile,
        "/exit" => Command::Exit,
        "/help" => Command::Help,
        _ => Command::Unknown,
    }
}