use std::time::Duration;

use color_eyre::{eyre::Context, Result};
use crossterm::event;
use itertools::Itertools;

use ratatui::{
    buffer::Buffer,
    crossterm::event::{Event, KeyCode, KeyEvent, KeyEventKind},
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Tabs, Widget},
    DefaultTerminal, Frame,
};
use strum::{Display, EnumIter, FromRepr, IntoEnumIterator};

use crate::{
    tabs::{AboutTab, ChatTab, DirectMessagesTab, GlobalChatTab},
    THEME,
};

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct App {
    mode: Mode,
    tab: Tab,
    username: String,
    about_tab: AboutTab,
    chat_tab: ChatTab,
    direct_messages_tab: DirectMessagesTab,
    global_chat_tab: GlobalChatTab,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum Mode {
    #[default]
    Start,
    Running,
    Quit,
}

#[derive(Debug, Clone, Copy, Default, Display, EnumIter, FromRepr, PartialEq, Eq)]
enum Tab {
    #[default]
    About,
    GlobalChat,
    Chat,
    DirectMessages,
}

impl App {
    /// Run the app until the user quits.
    pub fn run(mut self, mut terminal: DefaultTerminal) -> Result<()> {
        self.mode = Mode::Start;

        while self.is_running() {
            terminal
                .draw(|frame| self.draw(frame))
                .wrap_err("terminal.draw")?;
            self.handle_events()?;
        }
        Ok(())
    }

    fn is_running(&self) -> bool {
        self.mode != Mode::Quit
    }

    /// Draw a single frame of the app.
    fn draw(&self, frame: &mut Frame) {
        if self.mode == Mode::Start {
            self.draw_startup(frame);
        } else {
            frame.render_widget(self, frame.area());
        }
    }

    /**
     * 
     */
    fn draw_startup(&self, frame: &mut Frame) {
        let size = frame.area();
        let block = Block::default().borders(Borders::ALL).title("Welcome to SwapBytes!");

        let input = Paragraph::new(self.username.as_str())
            .style(Style::default().fg(THEME.logo.foreground))
            .block(Block::default().borders(Borders::ALL).title("Please enter a username:"));

        let layout = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
                .split(size);

        frame.render_widget(block, layout[0]);
        frame.render_widget(input, layout[1]);
    }

    /// Handle events from the terminal.
    ///
    /// This function is called once per frame, The events are polled from the stdin with timeout of
    /// 1/50th of a second. This was chosen to try to match the default frame rate of a GIF in VHS.
    fn handle_events(&mut self) -> Result<()> {
        let timeout = Duration::from_secs_f64(1.0 / 50.0);
        if !event::poll(timeout)? {
            return Ok(());
        }
        match event::read()? {
            Event::Key(key) if key.kind == KeyEventKind::Press => self.handle_key_press(key),
            _ => {}
        }
        Ok(())
    }

    fn handle_key_press(&mut self, key: KeyEvent) {
        match self.mode {
            Mode::Start => match key.code {
                KeyCode::Char(c) => {
                    self.username.push(c);
                }
                KeyCode::Backspace => {
                    self.username.pop();
                }
                KeyCode::Enter => {
                    if !self.username.is_empty() {
                        self.mode = Mode::Running; // switch to main app once username is entered
                    }
                }
                KeyCode::Esc => self.mode = Mode::Quit,
                _ => {}
            },
            Mode::Running => match key.code {
                KeyCode::Char('q') | KeyCode::Esc => self.mode = Mode::Quit,
                KeyCode::Char('h') | KeyCode::Left => self.prev_tab(),
                KeyCode::Char('l') | KeyCode::Right => self.next_tab(),
                KeyCode::Char('k') | KeyCode::Up => self.prev(),
                KeyCode::Char('j') | KeyCode::Down => self.next(),
                _ => {}
            },
            Mode::Quit => {}
        };
    }

    fn prev(&mut self) {
        match self.tab {
            Tab::About => self.about_tab.prev_row(),
            Tab::Chat => self.chat_tab.prev_row(),
            Tab::DirectMessages => self.direct_messages_tab.prev_row(),
            Tab::GlobalChat => self.global_chat_tab.prev_row(),
        }
    }

    fn next(&mut self) {
        match self.tab {
            Tab::About => self.about_tab.next_row(),
            Tab::Chat => self.chat_tab.next_row(),
            Tab::DirectMessages => self.direct_messages_tab.next_row(),
            Tab::GlobalChat => self.global_chat_tab.next_row(),
        }
    }

    fn prev_tab(&mut self) {
        self.tab = self.tab.prev();
    }

    fn next_tab(&mut self) {
        self.tab = self.tab.next();
    }
}

/// Implement Widget for &App rather than for App as we would otherwise have to clone or copy the
/// entire app state on every frame. For this example, the app state is small enough that it doesn't
/// matter, but for larger apps this can be a significant performance improvement.
impl Widget for &App {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let vertical = Layout::vertical([
            Constraint::Length(1),
            Constraint::Min(0),
            Constraint::Length(1),
        ]);
        let [title_bar, tab, bottom_bar] = vertical.areas(area);

        Block::new().style(THEME.root).render(area, buf);
        self.render_title_bar(title_bar, buf);
        self.render_selected_tab(tab, buf);
        App::render_bottom_bar(bottom_bar, buf);
    }
}

impl App {
    fn render_title_bar(&self, area: Rect, buf: &mut Buffer) {
        let layout = Layout::horizontal([Constraint::Min(0), Constraint::Length(43)]);
        let [title, tabs] = layout.areas(area);

        Span::styled(" SwapBytes ", THEME.app_title).render(title, buf);
        let titles = Tab::iter().map(Tab::title);
        Tabs::new(titles)
            .style(THEME.tabs)
            .highlight_style(THEME.tabs_selected)
            .select(self.tab as usize)
            .divider("")
            .padding(" ", " ")
            .render(tabs, buf);
    }

    fn render_selected_tab(&self, area: Rect, buf: &mut Buffer) {
        match self.tab {
            Tab::About => self.about_tab.render(area, buf, &self.username),
            Tab::Chat => self.chat_tab.render(area, buf, &self.username),
            Tab::DirectMessages => self.direct_messages_tab.render(area, buf),
            Tab::GlobalChat => self.global_chat_tab.render(area, buf),
        };
    }

    fn render_bottom_bar(area: Rect, buf: &mut Buffer) {
        let keys = [
            ("H/←", "Left"),
            ("L/→", "Right"),
            ("K/↑", "Up"),
            ("J/↓", "Down"),
            ("Q/Esc", "Quit"),
        ];
        let spans = keys
            .iter()
            .flat_map(|(key, desc)| {
                let key = Span::styled(format!(" {key} "), THEME.key_binding.key);
                let desc = Span::styled(format!(" {desc} "), THEME.key_binding.description);
                [key, desc]
            })
            .collect_vec();
        Line::from(spans)
            .centered()
            .style((Color::Indexed(236), Color::Indexed(232)))
            .render(area, buf);
    }
}

/**
 * Implements the tabs of the SwapByte application, also
 * handles all the logic for the tabs (next, prev).
 */
impl Tab {
    // gets the next tab.
    fn next(self) -> Self {
        let current_index = self as usize;
        let next_index = current_index.saturating_add(1);
        Self::from_repr(next_index).unwrap_or(self)
    }

    // gets the previous tab.
    fn prev(self) -> Self {
        let current_index = self as usize;
        let prev_index = current_index.saturating_sub(1);
        Self::from_repr(prev_index).unwrap_or(self)
    }

    // formats the title of the tab.
    fn title(self) -> String {
        match self {
            Self::About => String::new(),
            tab => format!(" {tab} "),
        }
    }
}