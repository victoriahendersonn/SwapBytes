use ratatui::{
    buffer::Buffer,
    layout::Rect,
    widgets::Widget,
};

use crate::THEME;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChatTab {
    row_index: usize,
    username: String,
}

pub struct Topic {
    name: String,
    messages: Vec<String>,
    new_message: String,
}

pub struct ChatTopics {
    topics: Vec<Topic>,
    selected_topic: usize,
}

impl ChatTab {
    pub fn prev_row(&mut self) {
        self.row_index = self.row_index.saturating_sub(1);
    }

    pub fn next_row(&mut self) {
        self.row_index = self.row_index.saturating_add(1);
    }

    pub fn render(&self, area: Rect, buf: &mut Buffer, username: &str) {
        render_chat(area, buf, username);
    }
}

fn render_chat(area: Rect, buf: &mut Buffer, username: &str) {
    
}