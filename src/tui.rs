/// sqlrustler TUI — cowboy-themed interactive interface.
/// Launched when `sqlrustler` is run with no arguments.
/// After the user fills in all details and confirms, the TUI restores the terminal
/// and hands off to the normal export/import/bench pipeline.
use anyhow::{bail, Result};
use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Wrap},
    Frame, Terminal,
};
use std::io;
use std::time::Duration;

// ── Brand colours ─────────────────────────────────────────────────────────────
const GOLD: Color = Color::Yellow;
const CYAN: Color = Color::Cyan;
const DIM: Color = Color::DarkGray;
const ERR: Color = Color::Red;

// ── ASCII banner ──────────────────────────────────────────────────────────────
const BANNER_LINES: &[&str] = &[
    r" ____  ___  __    ____  __  __  ____  ____  __    ____  ____",
    r"(_  _)/ __)(  )  ( ___)(  )(  )/ ___)(_  _)(  )  ( ___)(  _ \",
    r"  )(  \__ \ )(__  )__)  )(__)( \___ \ _)(_  )(__(  )__)  )   /",
    r" (__) (___/(____)(____)(______)(____/(____)(____)(____)(_)\_)",
];
const TAGLINE: &str = "BACKUP  ★  EXPORT  ★  RESTORE";

// ── App state machine ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Screen {
    Welcome,
    MainMenu,
    Connection,
    ExportOptions,
    ImportOptions,
    BenchOptions,
    Confirm,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Operation {
    Export,
    Import,
    Bench,
}

#[derive(Debug, Clone, PartialEq)]
enum AuthMethod {
    AzureSession, // `az account get-access-token` at run time
    Manual,       // SQL username + password entered in the form
}

struct App {
    screen: Screen,
    operation: Option<Operation>,
    auth_method: AuthMethod,

    // Connection
    host: String,
    database: String,
    user: String,
    password: String,

    // Export options
    output_path: String,
    parallel: String,
    export_batch: String,
    exclude_tables: String,
    schema_only: bool,
    compression: String,

    // Import options
    input_path: String,
    import_batch: String,
    drop_existing: bool,
    data_only: bool,

    // Bench options
    bench_output: String,
    compare_sqlpackage: bool,

    // Navigation
    menu_cursor: usize,
    active_field: usize,

    // Outcome
    should_quit: bool,
    should_run: bool,
    error_msg: Option<String>,
}

impl App {
    fn new() -> Self {
        Self {
            screen: Screen::Welcome,
            operation: None,
            auth_method: AuthMethod::AzureSession,
            host: String::new(),
            database: String::new(),
            user: String::new(),
            password: String::new(),
            output_path: String::new(),
            parallel: num_cpus::get().to_string(),
            export_batch: "10000".to_string(),
            exclude_tables: String::new(),
            schema_only: false,
            compression: "3".to_string(),
            input_path: String::new(),
            import_batch: "1000".to_string(),
            drop_existing: false,
            data_only: false,
            bench_output: String::new(),
            compare_sqlpackage: false,
            menu_cursor: 0,
            active_field: 0,
            should_quit: false,
            should_run: false,
            error_msg: None,
        }
    }

    /// Number of tab-stoppable fields on the current screen.
    fn field_count(&self) -> usize {
        match self.screen {
            Screen::Connection => {
                if self.auth_method == AuthMethod::Manual { 4 } else { 2 }
            }
            Screen::ExportOptions => 6,
            Screen::ImportOptions => 5,
            Screen::BenchOptions => 2,
            _ => 0,
        }
    }

    fn next_field(&mut self) {
        let n = self.field_count();
        if n > 0 { self.active_field = (self.active_field + 1) % n; }
    }

    fn prev_field(&mut self) {
        let n = self.field_count();
        if n > 0 { self.active_field = (self.active_field + n - 1) % n; }
    }

    /// Active text field on connection screen (returns mutable ref).
    fn conn_field_mut(&mut self) -> Option<&mut String> {
        match (self.active_field, &self.auth_method) {
            (0, _) => Some(&mut self.host),
            (1, _) => Some(&mut self.database),
            (2, AuthMethod::Manual) => Some(&mut self.user),
            (3, AuthMethod::Manual) => Some(&mut self.password),
            _ => None,
        }
    }

    fn export_field_mut(&mut self) -> Option<&mut String> {
        match self.active_field {
            0 => Some(&mut self.output_path),
            1 => Some(&mut self.parallel),
            2 => Some(&mut self.export_batch),
            3 => Some(&mut self.exclude_tables),
            4 => Some(&mut self.compression),
            _ => None, // 5 = schema_only toggle
        }
    }

    fn import_field_mut(&mut self) -> Option<&mut String> {
        match self.active_field {
            0 => Some(&mut self.input_path),
            1 => Some(&mut self.parallel),
            2 => Some(&mut self.import_batch),
            _ => None, // 3 = drop_existing toggle, 4 = data_only toggle
        }
    }
}

// ── Entry point ───────────────────────────────────────────────────────────────

pub async fn run_tui() -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;

    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_loop(&mut terminal).await;

    // Always restore terminal
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

async fn run_loop<B: ratatui::backend::Backend>(terminal: &mut Terminal<B>) -> Result<()>
where
    <B as ratatui::backend::Backend>::Error: Send + Sync + 'static,
{
    let mut app = App::new();

    loop {
        terminal.draw(|f| draw(&app, f))?;

        if event::poll(Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                if key.kind != KeyEventKind::Press {
                    continue;
                }
                // Global quit
                if key.code == KeyCode::Char('c')
                    && key.modifiers.contains(KeyModifiers::CONTROL)
                {
                    return Ok(());
                }
                handle_key(&mut app, key.code, key.modifiers);
            }
        }

        if app.should_quit {
            return Ok(());
        }

        if app.should_run {
            // Terminal already restored by the caller after this function returns.
            // Re-enable raw mode briefly so we see clean output, then hand off.
            let _ = terminal; // terminal restored by run_tui() caller; we just need to exit the loop
            return execute_operation(app).await;
        }
    }
}

// ── Keyboard handler ──────────────────────────────────────────────────────────

fn handle_key(app: &mut App, code: KeyCode, mods: KeyModifiers) {
    app.error_msg = None;

    match app.screen {
        Screen::Welcome => {
            app.screen = Screen::MainMenu;
        }

        Screen::MainMenu => match code {
            KeyCode::Up | KeyCode::Char('k') => {
                if app.menu_cursor > 0 { app.menu_cursor -= 1; }
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if app.menu_cursor < 3 { app.menu_cursor += 1; }
            }
            KeyCode::Enter => match app.menu_cursor {
                0 => { app.operation = Some(Operation::Export); app.screen = Screen::Connection; app.active_field = 0; }
                1 => { app.operation = Some(Operation::Import); app.screen = Screen::Connection; app.active_field = 0; }
                2 => { app.operation = Some(Operation::Bench); app.screen = Screen::Connection; app.active_field = 0; }
                _ => { app.should_quit = true; }
            },
            KeyCode::Char('q') => { app.should_quit = true; }
            _ => {}
        },

        Screen::Connection => match code {
            KeyCode::Esc => { app.screen = Screen::MainMenu; app.active_field = 0; }
            KeyCode::Tab => app.next_field(),
            KeyCode::BackTab => app.prev_field(),
            KeyCode::Char('a') if mods.contains(KeyModifiers::ALT) => {
                // Alt+A toggles auth method
                app.auth_method = if app.auth_method == AuthMethod::AzureSession {
                    AuthMethod::Manual
                } else {
                    AuthMethod::AzureSession
                };
                app.active_field = 0;
            }
            KeyCode::F(1) => {
                app.auth_method = AuthMethod::AzureSession;
                app.active_field = 0;
            }
            KeyCode::F(2) => {
                app.auth_method = AuthMethod::Manual;
                app.active_field = 0;
            }
            KeyCode::Enter => {
                if validate_connection(app) {
                    app.screen = match app.operation {
                        Some(Operation::Export) => Screen::ExportOptions,
                        Some(Operation::Import) => Screen::ImportOptions,
                        Some(Operation::Bench) => Screen::BenchOptions,
                        None => Screen::MainMenu,
                    };
                    app.active_field = 0;
                }
            }
            KeyCode::Backspace => {
                if let Some(f) = app.conn_field_mut() {
                    f.pop();
                }
            }
            KeyCode::Char(c) => {
                if let Some(f) = app.conn_field_mut() {
                    f.push(c);
                }
            }
            _ => {}
        },

        Screen::ExportOptions => match code {
            KeyCode::Esc => { app.screen = Screen::Connection; app.active_field = 0; }
            KeyCode::Tab => app.next_field(),
            KeyCode::BackTab => app.prev_field(),
            KeyCode::Char(' ') if app.active_field == 5 => {
                app.schema_only = !app.schema_only;
            }
            KeyCode::Enter => {
                app.screen = Screen::Confirm;
            }
            KeyCode::Backspace => {
                if let Some(f) = app.export_field_mut() { f.pop(); }
            }
            KeyCode::Char(c) => {
                if let Some(f) = app.export_field_mut() { f.push(c); }
            }
            _ => {}
        },

        Screen::ImportOptions => match code {
            KeyCode::Esc => { app.screen = Screen::Connection; app.active_field = 0; }
            KeyCode::Tab => app.next_field(),
            KeyCode::BackTab => app.prev_field(),
            KeyCode::Char(' ') if app.active_field == 3 => {
                app.drop_existing = !app.drop_existing;
            }
            KeyCode::Char(' ') if app.active_field == 4 => {
                app.data_only = !app.data_only;
            }
            KeyCode::Enter => {
                app.screen = Screen::Confirm;
            }
            KeyCode::Backspace => {
                if let Some(f) = app.import_field_mut() { f.pop(); }
            }
            KeyCode::Char(c) => {
                if let Some(f) = app.import_field_mut() { f.push(c); }
            }
            _ => {}
        },

        Screen::BenchOptions => match code {
            KeyCode::Esc => { app.screen = Screen::Connection; app.active_field = 0; }
            KeyCode::Tab => app.next_field(),
            KeyCode::BackTab => app.prev_field(),
            KeyCode::Char(' ') if app.active_field == 1 => {
                app.compare_sqlpackage = !app.compare_sqlpackage;
            }
            KeyCode::Enter => { app.screen = Screen::Confirm; }
            KeyCode::Backspace => {
                if app.active_field == 0 { app.bench_output.pop(); }
            }
            KeyCode::Char(c) => {
                if app.active_field == 0 { app.bench_output.push(c); }
            }
            _ => {}
        },

        Screen::Confirm => match code {
            KeyCode::Esc => {
                app.screen = match app.operation {
                    Some(Operation::Export) => Screen::ExportOptions,
                    Some(Operation::Import) => Screen::ImportOptions,
                    Some(Operation::Bench) => Screen::BenchOptions,
                    None => Screen::MainMenu,
                };
            }
            KeyCode::Enter => { app.should_run = true; }
            KeyCode::Char('q') => { app.should_quit = true; }
            _ => {}
        },
    }
}

fn validate_connection(app: &mut App) -> bool {
    if app.host.trim().is_empty() {
        app.error_msg = Some("Whoa! Server host can't be empty, partner.".to_string());
        app.active_field = 0;
        return false;
    }
    if app.database.trim().is_empty() {
        app.error_msg = Some("Hold yer horses — database name is required.".to_string());
        app.active_field = 1;
        return false;
    }
    if app.auth_method == AuthMethod::Manual && app.user.trim().is_empty() {
        app.error_msg = Some("A cowboy always has a name. Enter your username.".to_string());
        app.active_field = 2;
        return false;
    }
    true
}

// ── Drawing ───────────────────────────────────────────────────────────────────

fn draw(app: &App, f: &mut Frame) {
    let area = f.area();

    // Dark background
    f.render_widget(
        Block::default().style(Style::default().bg(Color::Black)),
        area,
    );

    match &app.screen {
        Screen::Welcome => draw_welcome(app, f, area),
        Screen::MainMenu => draw_main_menu(app, f, area),
        Screen::Connection => draw_connection(app, f, area),
        Screen::ExportOptions => draw_export_options(app, f, area),
        Screen::ImportOptions => draw_import_options(app, f, area),
        Screen::BenchOptions => draw_bench_options(app, f, area),
        Screen::Confirm => draw_confirm(app, f, area),
    }
}

fn draw_banner(f: &mut Frame, area: Rect) {
    let banner_text: Vec<Line> = BANNER_LINES
        .iter()
        .map(|l| Line::from(Span::styled(*l, Style::default().fg(CYAN).add_modifier(Modifier::BOLD))))
        .chain(std::iter::once(Line::from(Span::styled(
            format!("{:^62}", TAGLINE),
            Style::default().fg(GOLD).add_modifier(Modifier::BOLD),
        ))))
        .collect();

    f.render_widget(
        Paragraph::new(banner_text).alignment(Alignment::Center),
        area,
    );
}

fn draw_welcome(app: &App, f: &mut Frame, area: Rect) {
    let _ = app;
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage(20),
            Constraint::Length(7),
            Constraint::Length(2),
            Constraint::Length(3),
            Constraint::Min(0),
        ])
        .split(area);

    draw_banner(f, chunks[1]);

    f.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled("Howdy, partner! ", Style::default().fg(GOLD).add_modifier(Modifier::BOLD)),
            Span::styled("Ready to wrangle some data?", Style::default().fg(Color::White)),
        ]))
        .alignment(Alignment::Center),
        chunks[2],
    );

    f.render_widget(
        Paragraph::new(Line::from(Span::styled(
            "Press any key to saddle up...",
            Style::default().fg(DIM).add_modifier(Modifier::ITALIC),
        )))
        .alignment(Alignment::Center),
        chunks[3],
    );
}

fn draw_main_menu(app: &App, f: &mut Frame, area: Rect) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(7),
            Constraint::Length(1),
            Constraint::Length(8),
            Constraint::Min(0),
            Constraint::Length(1),
        ])
        .split(area);

    draw_banner(f, chunks[1]);

    let items: Vec<(&str, &str)> = vec![
        ("🤠", "Export database to .rustler"),
        ("📦", "Import .rustler into database"),
        ("⏱ ", "Benchmark vs sqlpackage"),
        ("🚪", "Quit"),
    ];

    let list_items: Vec<ListItem> = items
        .iter()
        .enumerate()
        .map(|(i, (icon, label))| {
            let selected = i == app.menu_cursor;
            let style = if selected {
                Style::default().fg(Color::Black).bg(CYAN).add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::White)
            };
            let prefix = if selected { " ▶  " } else { "    " };
            ListItem::new(Line::from(vec![
                Span::styled(prefix, style),
                Span::styled(*icon, style),
                Span::styled(format!("  {label}"), style),
            ]))
        })
        .collect();

    let menu_block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(GOLD))
        .title(Span::styled(" What'll it be, partner? ", Style::default().fg(GOLD).add_modifier(Modifier::BOLD)));

    let menu_width = 48u16;
    let menu_height = 8u16;
    let menu_area = centered_rect(menu_width, menu_height, area);
    f.render_widget(Clear, menu_area);
    f.render_widget(List::new(list_items).block(menu_block), menu_area);

    f.render_widget(
        Paragraph::new(Line::from(Span::styled(
            "  [↑↓] navigate   [Enter] select   [q] quit  ",
            Style::default().fg(DIM),
        )))
        .alignment(Alignment::Center),
        chunks[5],
    );
}

fn draw_connection(app: &App, f: &mut Frame, area: Rect) {
    let op_label = match &app.operation {
        Some(Operation::Export) => "Export",
        Some(Operation::Import) => "Import",
        Some(Operation::Bench) => "Benchmark",
        None => "",
    };

    let title = format!(" 🤠 {op_label} — Connection ");
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(GOLD))
        .title(Span::styled(title, Style::default().fg(GOLD).add_modifier(Modifier::BOLD)));

    let inner_width = 56u16;
    let inner_height = if app.auth_method == AuthMethod::Manual { 16 } else { 12 };
    let box_area = centered_rect(inner_width, inner_height, area);
    f.render_widget(Clear, box_area);
    f.render_widget(block, box_area);

    let inner = inner_rect(box_area);
    let mut lines: Vec<Line> = Vec::new();

    // Auth method toggle
    let az_style = if app.auth_method == AuthMethod::AzureSession {
        Style::default().fg(Color::Black).bg(CYAN).add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(DIM)
    };
    let manual_style = if app.auth_method == AuthMethod::Manual {
        Style::default().fg(Color::Black).bg(GOLD).add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(DIM)
    };

    lines.push(Line::from(vec![
        Span::styled(" Auth: ", Style::default().fg(Color::White)),
        Span::styled(" [F1] Azure session ", az_style),
        Span::styled("  ", Style::default()),
        Span::styled(" [F2] Manual ", manual_style),
    ]));
    lines.push(Line::from(""));

    // Fields
    let fields: Vec<(&str, &str, bool)> = if app.auth_method == AuthMethod::Manual {
        vec![
            ("Host    ", &app.host, false),
            ("Database", &app.database, false),
            ("User    ", &app.user, false),
            ("Password", &app.password, true),
        ]
    } else {
        vec![
            ("Host    ", &app.host, false),
            ("Database", &app.database, false),
        ]
    };

    for (i, (label, value, is_password)) in fields.iter().enumerate() {
        let active = i == app.active_field;
        let border_style = if active {
            Style::default().fg(CYAN).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(DIM)
        };
        let display = if *is_password {
            "•".repeat(value.len())
        } else {
            value.to_string()
        };
        let cursor = if active { "█" } else { "" };
        lines.push(Line::from(vec![
            Span::styled(format!(" {label}: "), Style::default().fg(Color::White)),
            Span::styled(
                format!("{display}{cursor}"),
                border_style,
            ),
        ]));
    }

    if app.auth_method == AuthMethod::AzureSession {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(
            "  ✓ Token fetched via `az account get-access-token`",
            Style::default().fg(Color::Green).add_modifier(Modifier::ITALIC),
        )));
    }

    lines.push(Line::from(""));

    if let Some(err) = &app.error_msg {
        lines.push(Line::from(Span::styled(
            format!(" ⚠  {err}"),
            Style::default().fg(ERR).add_modifier(Modifier::BOLD),
        )));
    }

    lines.push(Line::from(Span::styled(
        " [Tab] next   [F1/F2] auth   [Enter] continue   [Esc] back",
        Style::default().fg(DIM),
    )));

    f.render_widget(Paragraph::new(lines), inner);
}

fn draw_export_options(app: &App, f: &mut Frame, area: Rect) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(GOLD))
        .title(Span::styled(
            " 🤠 Export Options ",
            Style::default().fg(GOLD).add_modifier(Modifier::BOLD),
        ));

    let box_area = centered_rect(60, 18, area);
    f.render_widget(Clear, box_area);
    f.render_widget(block, box_area);

    let inner = inner_rect(box_area);
    let output_display = if app.output_path.is_empty() {
        format!("<database>_<timestamp>.rustler{}", if app.active_field == 0 { "█" } else { "" })
    } else {
        format!("{}{}", app.output_path, if app.active_field == 0 { "█" } else { "" })
    };

    let fields: Vec<(&str, String, bool)> = vec![
        ("Output path  ", output_display, false),
        ("Parallel (-j)", format!("{}{}", app.parallel, if app.active_field == 1 { "█" } else { "" }), false),
        ("Batch size   ", format!("{}{}", app.export_batch, if app.active_field == 2 { "█" } else { "" }), false),
        ("Exclude tables (globs)", format!("{}{}", app.exclude_tables, if app.active_field == 3 { "█" } else { "" }), false),
        ("Compression lvl (1-22)", format!("{}{}", app.compression, if app.active_field == 4 { "█" } else { "" }), false),
        ("Schema only  ", format!("[{}]  (Space to toggle)", if app.schema_only { "✓" } else { " " }), true),
    ];

    let mut lines: Vec<Line> = Vec::new();
    for (i, (label, value, is_toggle)) in fields.iter().enumerate() {
        let active = i == app.active_field;
        let val_style = if active {
            Style::default().fg(CYAN).add_modifier(Modifier::BOLD)
        } else if *is_toggle && app.schema_only {
            Style::default().fg(Color::Green)
        } else {
            Style::default().fg(Color::White)
        };
        lines.push(Line::from(vec![
            Span::styled(format!(" {label}: "), Style::default().fg(Color::White)),
            Span::styled(value.clone(), val_style),
        ]));
    }

    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        " [Tab] next   [Space] toggle   [Enter] continue   [Esc] back",
        Style::default().fg(DIM),
    )));

    f.render_widget(Paragraph::new(lines), inner);
}

fn draw_import_options(app: &App, f: &mut Frame, area: Rect) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(GOLD))
        .title(Span::styled(
            " 🤠 Import Options ",
            Style::default().fg(GOLD).add_modifier(Modifier::BOLD),
        ));

    let box_area = centered_rect(60, 16, area);
    f.render_widget(Clear, box_area);
    f.render_widget(block, box_area);
    let inner = inner_rect(box_area);

    let fields: Vec<(&str, String, bool)> = vec![
        ("Input path    ", format!("{}{}", app.input_path, if app.active_field == 0 { "█" } else { "" }), false),
        ("Parallel (-j) ", format!("{}{}", app.parallel, if app.active_field == 1 { "█" } else { "" }), false),
        ("Batch size    ", format!("{}{}", app.import_batch, if app.active_field == 2 { "█" } else { "" }), false),
        ("Drop existing ", format!("[{}]  (Space to toggle)", if app.drop_existing { "✓" } else { " " }), true),
        ("Data only     ", format!("[{}]  (Space to toggle)", if app.data_only { "✓" } else { " " }), true),
    ];

    let mut lines: Vec<Line> = Vec::new();
    for (i, (label, value, is_toggle)) in fields.iter().enumerate() {
        let active = i == app.active_field;
        let toggled = (*is_toggle) && ((i == 3 && app.drop_existing) || (i == 4 && app.data_only));
        let val_style = if active {
            Style::default().fg(CYAN).add_modifier(Modifier::BOLD)
        } else if toggled {
            Style::default().fg(Color::Green)
        } else {
            Style::default().fg(Color::White)
        };
        lines.push(Line::from(vec![
            Span::styled(format!(" {label}: "), Style::default().fg(Color::White)),
            Span::styled(value.clone(), val_style),
        ]));
    }

    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        " [Tab] next   [Space] toggle   [Enter] continue   [Esc] back",
        Style::default().fg(DIM),
    )));

    f.render_widget(Paragraph::new(lines), inner);
}

fn draw_bench_options(app: &App, f: &mut Frame, area: Rect) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(GOLD))
        .title(Span::styled(
            " 🤠 Benchmark Options ",
            Style::default().fg(GOLD).add_modifier(Modifier::BOLD),
        ));

    let box_area = centered_rect(60, 10, area);
    f.render_widget(Clear, box_area);
    f.render_widget(block, box_area);
    let inner = inner_rect(box_area);

    let output_display = if app.bench_output.is_empty() {
        format!("<auto>{}", if app.active_field == 0 { "█" } else { "" })
    } else {
        format!("{}{}", app.bench_output, if app.active_field == 0 { "█" } else { "" })
    };

    let lines = vec![
        Line::from(vec![
            Span::styled(" Output path:        ", Style::default().fg(Color::White)),
            Span::styled(output_display, if app.active_field == 0 { Style::default().fg(CYAN).add_modifier(Modifier::BOLD) } else { Style::default().fg(Color::White) }),
        ]),
        Line::from(vec![
            Span::styled(" Compare sqlpackage: ", Style::default().fg(Color::White)),
            Span::styled(
                format!("[{}]  (Space to toggle)", if app.compare_sqlpackage { "✓" } else { " " }),
                if app.compare_sqlpackage { Style::default().fg(Color::Green) } else { Style::default().fg(Color::White) },
            ),
        ]),
        Line::from(""),
        Line::from(Span::styled(
            " [Tab] next   [Space] toggle   [Enter] go   [Esc] back",
            Style::default().fg(DIM),
        )),
    ];

    f.render_widget(Paragraph::new(lines), inner);
}

fn draw_confirm(app: &App, f: &mut Frame, area: Rect) {
    let op_label = match &app.operation {
        Some(Operation::Export) => "Export",
        Some(Operation::Import) => "Import",
        Some(Operation::Bench) => "Benchmark",
        None => "Run",
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(GOLD))
        .title(Span::styled(
            format!(" 🤠 Ready to ride? — {op_label} "),
            Style::default().fg(GOLD).add_modifier(Modifier::BOLD),
        ));

    let box_area = centered_rect(62, 22, area);
    f.render_widget(Clear, box_area);
    f.render_widget(block, box_area);
    let inner = inner_rect(box_area);

    let auth_display = match app.auth_method {
        AuthMethod::AzureSession => "Azure session (az account get-access-token)".to_string(),
        AuthMethod::Manual => format!("SQL auth  user: {}", app.user),
    };

    let mut lines = vec![
        Line::from(Span::styled(" ── Connection ───────────────────────────────", Style::default().fg(DIM))),
        kv("  Host    ", &app.host),
        kv("  Database", &app.database),
        kv("  Auth    ", &auth_display),
        Line::from(""),
    ];

    match &app.operation {
        Some(Operation::Export) => {
            lines.push(Line::from(Span::styled(" ── Export ───────────────────────────────────", Style::default().fg(DIM))));
            let out = if app.output_path.is_empty() { "<auto>.rustler".to_string() } else { app.output_path.clone() };
            let compress = format!("zstd level {}", app.compression);
            lines.push(kv("  Output   ", &out));
            lines.push(kv("  Parallel ", &app.parallel));
            lines.push(kv("  Batch    ", &app.export_batch));
            if !app.exclude_tables.is_empty() {
                lines.push(kv("  Exclude  ", &app.exclude_tables));
            }
            if app.schema_only { lines.push(kv("  Schema only", "yes")); }
            lines.push(kv("  Compress ", &compress));
        }
        Some(Operation::Import) => {
            lines.push(Line::from(Span::styled(" ── Import ───────────────────────────────────", Style::default().fg(DIM))));
            lines.push(kv("  Input    ", &app.input_path));
            lines.push(kv("  Parallel ", &app.parallel));
            lines.push(kv("  Batch    ", &app.import_batch));
            if app.drop_existing { lines.push(kv("  Drop DB  ", "yes — database will be recreated")); }
            if app.data_only { lines.push(kv("  Data only", "yes — DDL skipped")); }
        }
        Some(Operation::Bench) => {
            lines.push(Line::from(Span::styled(" ── Benchmark ────────────────────────────────", Style::default().fg(DIM))));
            let out = if app.bench_output.is_empty() { "<auto>".to_string() } else { app.bench_output.clone() };
            lines.push(kv("  Output   ", &out));
            if app.compare_sqlpackage { lines.push(kv("  Compare  ", "sqlpackage")); }
        }
        None => {}
    }

    lines.push(Line::from(""));
    lines.push(Line::from(vec![
        Span::styled(" ", Style::default()),
        Span::styled(" [Enter] Yeehaw! Let's go ", Style::default().fg(Color::Black).bg(Color::Green).add_modifier(Modifier::BOLD)),
        Span::styled("   ", Style::default()),
        Span::styled(" [Esc] Change something ", Style::default().fg(DIM)),
    ]));

    f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), inner);
}

fn kv(key: &'static str, val: &str) -> Line<'static> {
    Line::from(vec![
        Span::styled(key, Style::default().fg(DIM)),
        Span::styled(format!(": {val}"), Style::default().fg(Color::White)),
    ])
}

// ── Execute operation after TUI exits ─────────────────────────────────────────

async fn execute_operation(app: App) -> Result<()> {
    use crate::cli::ConnectArgs;

    // If Azure session auth, fetch token via az CLI
    let (aad_token, user, password) = match app.auth_method {
        AuthMethod::AzureSession => {
            println!("\n🤠 Fetching Azure session token via az CLI...");
            let token = get_azure_token().await?;
            (Some(token), None, None)
        }
        AuthMethod::Manual => {
            let pass = if app.password.is_empty() { None } else { Some(app.password.clone()) };
            (None, Some(app.user.clone()), pass)
        }
    };

    let conn = ConnectArgs {
        host: app.host.clone(),
        database: app.database.clone(),
        user,
        password,
        aad_token,
        port: 1433,
        trust_cert: false,
    };

    match &app.operation {
        Some(Operation::Export) => {
            use crate::cli::ExportArgs;
            let args = ExportArgs {
                conn,
                output: if app.output_path.is_empty() { None } else { Some(std::path::PathBuf::from(&app.output_path)) },
                parallel: app.parallel.parse().unwrap_or_else(|_| num_cpus::get()),
                batch_size: app.export_batch.parse().unwrap_or(10000),
                exclude_tables: if app.exclude_tables.is_empty() {
                    vec![]
                } else {
                    app.exclude_tables.split(',').map(str::trim).map(String::from).collect()
                },
                schema_only: app.schema_only,
                compression_level: app.compression.parse().unwrap_or(3),
            };
            println!("🤠 Rounding up rows from {}...\n", args.conn.host);
            crate::export::run(args).await?;
        }
        Some(Operation::Import) => {
            use crate::cli::ImportArgs;
            if app.input_path.is_empty() {
                bail!("No input archive path provided.");
            }
            let args = ImportArgs {
                conn,
                input: std::path::PathBuf::from(&app.input_path),
                parallel: app.parallel.parse().unwrap_or_else(|_| num_cpus::get()),
                batch_size: app.import_batch.parse().unwrap_or(1000),
                data_only: app.data_only,
                drop_existing: app.drop_existing,
            };
            println!("🤠 Riding the herd into the corral at {}...\n", args.conn.host);
            crate::import::run(args).await?;
        }
        Some(Operation::Bench) => {
            use crate::cli::BenchArgs;
            let args = BenchArgs {
                conn,
                output: if app.bench_output.is_empty() { None } else { Some(std::path::PathBuf::from(&app.bench_output)) },
                compare: if app.compare_sqlpackage { Some("sqlpackage".to_string()) } else { None },
            };
            println!("🤠 Let's see who's fastest in the West...\n");
            crate::bench::run(args).await?;
        }
        None => {}
    }

    println!("\n🤠 That's a wrap, cowpoke! Ride on.");
    Ok(())
}

/// Shell out to `az account get-access-token` to get a bearer token for Azure SQL.
async fn get_azure_token() -> Result<String> {
    let output = tokio::process::Command::new("az")
        .args([
            "account",
            "get-access-token",
            "--resource",
            "https://database.windows.net/",
            "--query",
            "accessToken",
            "--output",
            "tsv",
        ])
        .output()
        .await;

    match output {
        Ok(out) if out.status.success() => {
            let token = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if token.is_empty() {
                bail!("az returned an empty token. Are you logged in? Run: az login");
            }
            Ok(token)
        }
        Ok(out) => {
            let stderr = String::from_utf8_lossy(&out.stderr);
            bail!(
                "az account get-access-token failed.\n\
                 Make sure the Azure CLI is installed and you are logged in (`az login`).\n\
                 Error: {stderr}"
            )
        }
        Err(e) => {
            bail!(
                "Could not run `az`. Is the Azure CLI installed?\n\
                 Install: https://aka.ms/installazurecliwindows\n\
                 Error: {e}"
            )
        }
    }
}

// ── Layout helpers ────────────────────────────────────────────────────────────

/// Returns a centered rect of fixed width×height within `area`.
fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let x = area.x + area.width.saturating_sub(width) / 2;
    let y = area.y + area.height.saturating_sub(height) / 2;
    Rect {
        x,
        y,
        width: width.min(area.width),
        height: height.min(area.height),
    }
}

/// Inner area with 1-cell padding inside a bordered block.
fn inner_rect(outer: Rect) -> Rect {
    Rect {
        x: outer.x + 1,
        y: outer.y + 1,
        width: outer.width.saturating_sub(2),
        height: outer.height.saturating_sub(2),
    }
}
