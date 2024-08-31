mod app;
mod colors;
mod tabs;
mod theme;

use color_eyre::Result;
use app::App;

pub use self::{
    colors::color_from_oklab,
    theme::THEME,
};

/** 
 * This is the main function that will run the application and 
 * present users with the initial screen of the application, asking
 * them to choose a username before proceeding with the application.
 */
fn main() -> Result<()> {
    color_eyre::install()?;
    let terminal = ratatui::init();
    let app_result = App::default().run(terminal);
    ratatui::restore();
    app_result
}