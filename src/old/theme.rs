use ratatui::style::{Color, Modifier, Style};

pub struct Theme {
    pub root: Style,
    pub content: Style,
    pub app_title: Style,
    pub tabs: Style,
    pub tabs_selected: Style,
    pub borders: Style,
    pub description: Style,
    pub description_title: Style,
    pub key_binding: KeyBinding,
    pub logo: Logo,
}

pub struct Logo {
    pub foreground: Color,
    pub background: Color,
}

pub struct KeyBinding {
    pub key: Style,
    pub description: Style,
}

pub const THEME: Theme = Theme {
    root: Style::new().bg(BACKGROUND_DARK_GREEN),
    content: Style::new().bg(BORDER_GREEN).fg(BACKGROUND_DARK_GREEN),
    app_title: Style::new()
        .fg(TITLE_TEXT_DARK_GREEN)
        .bg(BORDER_GREEN),
    tabs: Style::new().fg(HEADING_TEXT_WHITE).bg(BORDER_GREEN),
    tabs_selected: Style::new()
        .fg(SELECTED_WHITE)
        .bg(SELECTED_TEXT_DARK_GREEN)
        .add_modifier(Modifier::BOLD)
        .add_modifier(Modifier::REVERSED),
    borders: Style::new().fg(BORDER_GREEN),
    description: Style::new().fg(BACKGROUND_DARK_GREEN).bg(BACKGROUND_DARK_GREEN),
    description_title: Style::new().fg(HEADING_TEXT_WHITE).add_modifier(Modifier::BOLD),
    logo: Logo {
        foreground: BORDER_GREEN,
        background: BACKGROUND_DARK_GREEN,
    },
    key_binding: KeyBinding {
        key: Style::new().fg(BLACK).bg(DARK_GRAY),
        description: Style::new().fg(DARK_GRAY).bg(BLACK),
    },
};

//const DARK_BLUE: Color = Color::Rgb(16, 24, 48);
//const LIGHT_BLUE: Color = Color::Rgb(64, 96, 192);
//const LIGHT_YELLOW: Color = Color::Rgb(192, 192, 96);
//const LIGHT_GREEN: Color = Color::Rgb(64, 192, 96);
//const LIGHT_RED: Color = Color::Rgb(192, 96, 96);
//const RED: Color = Color::Rgb(215, 0, 0);
const BLACK: Color = Color::Rgb(8, 8, 8); // not really black, often #080808
const DARK_GRAY: Color = Color::Rgb(68, 68, 68);
//const MID_GRAY: Color = Color::Rgb(128, 128, 128);
//const LIGHT_GRAY: Color = Color::Rgb(188, 188, 188);
//const WHITE: Color = Color::Rgb(238, 238, 238); // not really white, often #eeeeee

// Alien Isolation theme
const BACKGROUND_DARK_GREEN: Color = Color::Rgb(2, 36, 2); // hex #022402
const SELECTED_WHITE: Color = Color::Rgb(212, 223, 207); // hex #D4DFCF
const BORDER_GREEN: Color = Color::Rgb(0, 190, 110); // hex #00BE6E
//const BUTTON_GREEN: Color = Color::Rgb(0, 89, 58); // hex #00593A

const TITLE_TEXT_DARK_GREEN: Color = Color::Rgb(1, 46, 9); // hex #012E09
const HEADING_TEXT_WHITE: Color = Color::Rgb(212, 223, 207); // hex #D4DFCF
//const HEADING_TEXT_GREEN: Color = Color::Rgb(0, 164, 93); // hex #00A45D
const SELECTED_TEXT_DARK_GREEN: Color = Color::Rgb(43, 68, 41); // hex #2B4429
//const BUTTON_TEXT_LIGHT_GREEN: Color = Color::Rgb(0, 190, 255); // hex #00BEFF
