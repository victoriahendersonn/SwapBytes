use ratatui::{
    buffer::Buffer,
    layout::Rect,
    widgets::Widget,
};

use crate::THEME;

const SWAPBYTES_LOGO: [&str; 6] = [
"     _____                     ____        __           ",
"    / ___/      ______ _____  / __ )__  __/ /____  _____",
"    \\__ \\ | /| / / __ `/ __ \\/ __  / / / / __/ _ \\/ ___/",
"   ___/ / |/ |/ / /_/ / /_/ / /_/ / /_/ / /_/  __(__  ) ",
"  /____/|__/|__/\\__,_/ .___/_____/\\__, /\\__/\\___/____/  ",
"                    /_/          /____/                 ",
];


#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AboutTab {
    row_index: usize,
    username: String,
}

impl AboutTab {
    pub fn prev_row(&mut self) {
        self.row_index = self.row_index.saturating_sub(1);
    }

    pub fn next_row(&mut self) {
        self.row_index = self.row_index.saturating_add(1);
    }

    pub fn render(&self, area: Rect, buf: &mut Buffer, username: &str) {
        render_logo(self.row_index, area, buf);
        render_welcome( area, buf, username);
    }
}

pub fn render_logo(row_index: usize, area: Rect, buf: &mut Buffer) {
    let logo_height: usize = SWAPBYTES_LOGO.len();
    let logo_width = SWAPBYTES_LOGO[0].len();

    let vertical_padding = (area.height.saturating_sub(logo_height as u16)) / 2;
    let horizontal_padding = (area.width.saturating_sub(logo_width as u16)) / 2;

    let max_lines_to_render = (area.height as usize).min(logo_height - row_index);

    for (i, line) in SWAPBYTES_LOGO
        .iter()
        .skip(row_index) // Skip lines based on row_index for scrolling
        .take(max_lines_to_render) // Render only the visible lines
        .enumerate()
    {
        let y = area.y + i as u16 + vertical_padding; 
        let x = horizontal_padding; 
        buf.set_string(x, y, line, THEME.logo.foreground);
    }
}

pub fn render_welcome(area: Rect, buf: &mut Buffer, username: &str) {

    let welcome_message = format!("Welcome to Swapbytes, {}!", username);
    let welcome_message_width = welcome_message.len() as u16;

    let vertical_padding = (area.height - 1) / 2;
    let horizontal_padding = (area.width.saturating_sub(welcome_message_width)) / 2;

    buf.set_string(horizontal_padding, area.y + vertical_padding, welcome_message, THEME.logo.foreground);
}