use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Layout, Rect},
    widgets::Widget,
};

use crate::THEME;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GlobalChatTab {
    row_index: usize,
}

impl GlobalChatTab {
    pub fn prev_row(&mut self) {
        self.row_index = self.row_index.saturating_sub(1);
    }

    pub fn next_row(&mut self) {
        self.row_index = self.row_index.saturating_add(1);
    }
}

impl Widget for GlobalChatTab {
    fn render(self, area: Rect, buf: &mut Buffer) {
        
    }
}