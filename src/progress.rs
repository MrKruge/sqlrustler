use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use std::time::Duration;

pub fn make_export_bar(multi: &MultiProgress, table_fqn: &str) -> ProgressBar {
    let pb = multi.add(ProgressBar::new_spinner());
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} {prefix:.bold.dim} [{elapsed_precise}] {pos} rows {msg}",
        )
        .unwrap()
        .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
    );
    pb.set_prefix(table_fqn.to_string());
    pb.enable_steady_tick(Duration::from_millis(100));
    pb
}

pub fn make_import_bar(multi: &MultiProgress, table_fqn: &str, total_rows: u64) -> ProgressBar {
    let pb = multi.add(ProgressBar::new(total_rows));
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.blue} {prefix:.bold.dim} [{bar:30.cyan/blue}] {pos}/{len} rows ({eta})",
        )
        .unwrap()
        .progress_chars("=>-"),
    );
    pb.set_prefix(table_fqn.to_string());
    pb
}
