//! CSV Row Length Analyzer - Main Application
//! 
//! This is the entry point for the CSV row character count analyzer application.
//! It demonstrates how to use the csv_row_analyzer module to process CSV files.
//!
//! # Usage
//!
//! ```bash
//! # Basic usage (outputs reports to "reports" directory)
//! $ cargo run --release -- path/to/large_file.csv
//!
//! # With custom output directory
//! $ cargo run --release -- path/to/large_file.csv custom/output/dir
//! ```

// Import the analyzer module
mod csv_row_analyzer;
use csv_row_analyzer::csv_row_analyzer_main;


/// call from module
fn main() {
    csv_row_analyzer_main();
}
