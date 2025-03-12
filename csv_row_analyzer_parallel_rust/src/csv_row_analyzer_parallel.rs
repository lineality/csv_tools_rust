//! # CSV Row Character-Count Analyzer with Parallel Processing
//! 
//! A high-performance CSV file processor that analyzes character counts per row with multi-threaded
//! processing. It generates statistical reports including frequency distributions, outlier detection,
//! and page-equivalent metrics.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use std::env;
use std::process;
use std::thread;

// set approximate page length here:
const CHARS_PER_PAGE: usize = 3000;
const FLOAT_PAGE_SIZE: f64 = CHARS_PER_PAGE as f64; // Convert usize to f64
// Number of worker threads to use for processing
const WORKER_THREADS: usize = 8;

/// Represents the source of CSV files to process
enum InputSource {
    /// A single file to process
    SingleFile(String),
    /// A directory containing multiple CSV files to process
    Directory(String),
}

/// Entry for tracking row metadata
#[derive(Debug, Clone)]
struct RowEntry {
    /// The 1-based row number in the original file
    file_row: usize,
    /// The character count of this row
    char_count: usize,
}

/// Analyzes a CSV file to count characters per row and generate statistical reports.
/// 
/// This function processes the CSV file using multiple threads for better performance.
/// It splits the file into chunks, processes each chunk in parallel, and then combines
/// the results to generate comprehensive reports.
/// 
/// # Arguments
/// 
/// * `input_file_path` - Path to the input CSV file to analyze
/// * `output_directory_path` - Directory where report files will be saved (will be created if it doesn't exist)
/// 
/// # Returns
/// 
/// * `Result<(), io::Error>` - Ok(()) on success, or an Error if file operations fail
fn analyze_csv_row_lengths(
    input_file_path: impl AsRef<Path>, 
    output_directory_path: impl AsRef<Path>
) -> Result<(), io::Error> {
    // Ensure output directory exists
    fs::create_dir_all(&output_directory_path.as_ref())?;
    
    // Extract the basename from the input path
    let input_basename = extract_basename(&input_file_path)?;
    
    // Generate timestamp for unique report filenames
    let timestamp = generate_timestamp()?;
    
    // Prepare output paths for all reports
    let row_report_path = Path::new(output_directory_path.as_ref())
        .join(format!("{}_char_counts_report_{}.csv", input_basename, timestamp));
    let freq_report_path = Path::new(output_directory_path.as_ref())
        .join(format!("{}_value_counts_report_{}.csv", input_basename, timestamp));
    let outliers_report_path = Path::new(output_directory_path.as_ref())
        .join(format!("{}_md_outliers_report_{}.md", input_basename, timestamp));
    let pages_report_path = Path::new(output_directory_path.as_ref())
        .join(format!("{}_pages_valuecounts_report_{}.csv", input_basename, timestamp));
    let txt_report_path = Path::new(output_directory_path.as_ref())
        .join(format!("{}_txt_outliers_report_{}.txt", input_basename, timestamp));
    
    // Read the file once to get all lines as strings (resolving the Result)
    let file = File::open(input_file_path.as_ref())?;
    let reader = BufReader::new(file);
    let mut all_lines: Vec<(usize, String)> = Vec::new();
    let mut error_count: u64 = 0;
    
    // Read lines from file - convert 0-based index to 1-based file_row for human readability
    for (idx, line_result) in reader.lines().enumerate() {
        let file_row = idx + 1; // Convert to 1-based index for human readability
        match line_result {
            Ok(line) => all_lines.push((file_row, line)),
            Err(e) => {
                // Log error but continue
                eprintln!("Warning: Error reading file row {}: {}", file_row, e);
                error_count += 1;
            }
        }
    }
    
    // Now that we have all valid lines, we can divide them into chunks
    let lines_per_chunk = (all_lines.len() / WORKER_THREADS) + 1;
    let chunks: Vec<Vec<(usize, String)>> = all_lines
        .chunks(lines_per_chunk)
        .map(|chunk| chunk.to_vec())
        .collect();
    
    let total_lines = all_lines.len();
    println!("Processing {} lines with {} worker threads", total_lines, WORKER_THREADS);
    
    // Using threads with message passing instead of shared state
    let mut handles = Vec::with_capacity(chunks.len());
    
    for (chunk_index, chunk) in chunks.into_iter().enumerate() {
        println!("Spawning worker thread {} with {} lines", chunk_index, chunk.len());
        
        // Spawn a worker thread for this chunk
        let handle = thread::spawn(move || {
            // Thread-local collections
            let mut local_row_entries = Vec::with_capacity(chunk.len());
            let mut local_total_chars = 0;
            
            // Process all rows in this chunk locally
            for (file_row, line) in chunk {
                // Count characters in the current row
                let char_count = line.chars().count();
                
                // Store row entry with the original file_row (1-based)
                local_row_entries.push(RowEntry {
                    file_row,
                    char_count,
                });
                
                local_total_chars += char_count;
            }
            
            // Return the results directly without shared state
            (local_row_entries, local_total_chars)
        });
        
        handles.push(handle);
    }
    
    // Collect results from all threads
    let mut all_row_entries = Vec::with_capacity(total_lines);
    let mut total_chars = 0;
    
    for handle in handles {
        let (thread_entries, thread_chars) = handle.join().expect("Thread panicked");
        all_row_entries.extend(thread_entries);
        total_chars += thread_chars;
    }
    
    println!("All threads completed. Collected {} entries", all_row_entries.len());
    
    // Sort entries by original file row to maintain original file order
    all_row_entries.sort_by_key(|entry| entry.file_row);
    
    // Now assign data_index values sequentially
    // Data index is -1 for header row, then 0, 1, 2, etc. for data rows
    let row_entries: Vec<(usize, isize, usize)> = all_row_entries.iter().enumerate()
        .map(|(i, entry)| {
            // Determine data_index: -1 for header, then 0, 1, 2, etc.
            let data_index = if entry.file_row == 1 { -1 } else { (i as isize) - 1 };
            (entry.file_row, data_index, entry.char_count)
        })
        .collect();
    
    println!("Sorted entries and assigned data indices");
    
    // Create report files
    let mut row_report_file = File::create(&row_report_path)?;
    let mut freq_report_file = File::create(&freq_report_path)?;
    
    // Write headers to report files
    writeln!(row_report_file, "file_row,data_index,character_length")?;
    writeln!(freq_report_file, "character_length_of_rows,value_count")?;
    
    // Write row data to file
    for (file_row, data_index, char_count) in &row_entries {
        writeln!(row_report_file, "{},{},{}", file_row, data_index, char_count)?;
    }
    
    // Create a new report for character-length sorted data (descending)
    let length_report_path = Path::new(output_directory_path.as_ref())
        .join(format!("{}_length_sorted_report_{}.csv", input_basename, timestamp));
    let mut length_report_file = File::create(&length_report_path)?;

    // Write header to length-sorted report file
    writeln!(length_report_file, "file_row,data_index,character_length")?;

    // Create a sorted copy by character length (descending)
    let mut length_sorted_entries = row_entries.clone();
    length_sorted_entries.sort_by(|a, b| b.2.cmp(&a.2));  // Sort by char_count (descending)

    // Write length-sorted data to file with original indices
    for (file_row, data_index, char_count) in &length_sorted_entries {
        writeln!(length_report_file, "{},{},{}", file_row, data_index, char_count)?;
    }
    
    // Extract just the character counts for statistics
    let all_row_lengths: Vec<usize> = row_entries.iter()
        .map(|(_, _, char_count)| *char_count)
        .collect();
    
    // Calculate row length counts
    let mut row_length_counts: HashMap<usize, u64> = HashMap::new();
    for (_, _, char_count) in &row_entries {
        *row_length_counts.entry(*char_count).or_insert(0) += 1;
    }
    
    // Build row indices map (mapping from character count to vectors of file/data indices)
    let mut file_indices_map: HashMap<usize, Vec<usize>> = HashMap::new();
    let mut data_indices_map: HashMap<usize, Vec<isize>> = HashMap::new();
    
    for (file_row, data_index, char_count) in &row_entries {
        file_indices_map.entry(*char_count)
            .or_insert_with(Vec::new)
            .push(*file_row);
            
        data_indices_map.entry(*char_count)
            .or_insert_with(Vec::new)
            .push(*data_index);
    }
    
    // Convert the row length counts to a vector for sorting
    let mut length_counts_vec: Vec<(usize, u64)> = row_length_counts.iter()
        .map(|(&k, &v)| (k, v))
        .collect();
    
    // Sort by value (row length) in descending order
    length_counts_vec.sort_by(|a, b| b.0.cmp(&a.0));
    
    // Write frequency distribution to the second report
    for &(row_length, count) in &length_counts_vec {
        writeln!(freq_report_file, "{},{}", row_length, count)?;
    }
    
    // Write pages report directly
    let mut pages_report_file = File::create(&pages_report_path)?;
    
    // Write header to report file
    writeln!(pages_report_file, "page_length,pages_valuecount,percentage")?;
    
    // Calculate page lengths for each row (ceiling division to round up)
    let mut page_length_counts: HashMap<usize, u64> = HashMap::new();
    
    for (_, _, char_count) in &row_entries {
        // Calculate pages (round up: if char_count is 2001, it should be 2 pages)
        let pages = (*char_count + CHARS_PER_PAGE - 1) / CHARS_PER_PAGE;
        
        // Update frequency count
        *page_length_counts.entry(pages).or_insert(0) += 1;
    }
    
    // Convert HashMap to Vec for sorting
    let mut page_counts_vec: Vec<(usize, u64)> = page_length_counts.into_iter().collect();
    
    // Sort by page length in ascending order
    page_counts_vec.sort_by(|a, b| a.0.cmp(&b.0));
    
    // Calculate total rows for percentage
    let total_rows = row_entries.len() as f64;
    
    // Write frequency distribution to the report
    for (page_length, count) in &page_counts_vec {
        let percentage = (*count as f64 / total_rows) * 100.0;
        writeln!(pages_report_file, "{},{},{:.2}", page_length, count, percentage)?;
    }
    
    // Generate and write the outliers report
    generate_markdown_outliers_report(
        &outliers_report_path,
        &input_basename,
        &all_row_lengths,
        &length_counts_vec,
        row_entries.len() as u64,
        total_chars,
        error_count,
        &file_indices_map,
        &data_indices_map,
    )?;
    
    // Generate the text version of the outliers report for better readability
    generate_text_outliers_report(
        &txt_report_path,
        &input_basename,
        &all_row_lengths,
        &length_counts_vec,
        row_entries.len() as u64,
        total_chars,
        error_count,
        &file_indices_map,
        &data_indices_map,
    )?;
    
    Ok(())
}

/// Generates a plain text version of the outliers report with evenly spaced columns.
/// 
/// # Arguments
/// 
/// * `txt_report_path` - Path where the text report should be saved
/// * `input_basename` - Original filename basename for reporting
/// * `row_lengths` - Vector of all row lengths encountered
/// * `length_counts` - Vector of (length, count) pairs sorted by frequency
/// * `total_rows` - Total number of rows processed
/// * `total_chars` - Total number of characters across all rows
/// * `error_count` - Number of rows with reading errors
/// * `file_indices_map` - Map of row lengths to file row indices
/// * `data_indices_map` - Map of row lengths to data indices
/// 
/// # Returns
/// 
/// * `Result<(), io::Error>` - Ok(()) on success, or an Error if file operations fail
fn generate_text_outliers_report<P: AsRef<Path>>(
    txt_report_path: P,
    input_basename: &str,
    row_lengths: &[usize],
    length_counts: &[(usize, u64)],
    total_rows: u64,
    total_chars: usize,
    error_count: u64,
    file_indices_map: &HashMap<usize, Vec<usize>>,
    data_indices_map: &HashMap<usize, Vec<isize>>,
) -> Result<(), io::Error> {
    // Create the text report file
    let mut txt_file = File::create(txt_report_path)?;
    
    // Calculate descriptive statistics
    let stats = calculate_statistics(row_lengths);
    
    // Identify potential outliers - ensure all operands are f64
    let q1_f64 = stats.q1 as f64;
    let q3_f64 = stats.q3 as f64;
    let iqr = q3_f64 - q1_f64;
    let outlier_threshold_upper = q3_f64 + 1.5 * iqr;
    let outlier_threshold_lower = q1_f64 - 1.5 * iqr;
    
    // Write report header with fixed width
    writeln!(txt_file, "ROW LENGTH ANALYSIS FOR {}", input_basename)?;
    writeln!(txt_file, "{}", "=".repeat(50))?;
    writeln!(txt_file, "\nAnalysis performed on {} rows ({} with errors)", 
             total_rows, error_count)?;
    
    // Approx words and pages
    let estimated_words = total_chars / 5;  // Rough estimate: 5 chars per word on average
    let estimated_pages = total_chars / CHARS_PER_PAGE;  // Rough estimate: N chars per page
    
    // Write basic file statistics
    writeln!(txt_file, "\nFILE STATISTICS")?;
    writeln!(txt_file, "{}", "-".repeat(50))?;
    writeln!(txt_file, "Total Rows:                 {}", total_rows)?;
    writeln!(txt_file, "Total Characters:           {} (~{} words, ~{} pages)", 
             total_chars, estimated_words, estimated_pages)?;
    writeln!(txt_file, "Average Characters Per Row: {:.2} (~{:.1} words)", 
             total_chars as f64 / total_rows as f64, (total_chars as f64 / total_rows as f64) / 5.0)?;
    writeln!(txt_file, "Unique Row Lengths:         {}", length_counts.len())?;
    
    // Write descriptive statistics section
    writeln!(txt_file, "\nDESCRIPTIVE STATISTICS FOR ROW LENGTHS")?;
    writeln!(txt_file, "{}", "-".repeat(50))?;
    writeln!(txt_file, "Minimum:                 {} chars", stats.min)?;
    writeln!(txt_file, "Maximum:                 {} chars (~{} words, ~{:.1} pages)", 
             stats.max, stats.max / 5, stats.max as f64 / FLOAT_PAGE_SIZE)?;
    writeln!(txt_file, "Range:                   {} chars", stats.max - stats.min)?;
    writeln!(txt_file, "Mean:                    {:.2} chars", stats.mean)?;
    writeln!(txt_file, "Median:                  {} chars", stats.median)?;
    writeln!(txt_file, "25th Percentile (Q1):    {} chars", stats.q1)?;
    writeln!(txt_file, "75th Percentile (Q3):    {} chars", stats.q3)?;
    writeln!(txt_file, "Interquartile Range:     {} chars", stats.q3 - stats.q1)?;
    writeln!(txt_file, "Standard Deviation:      {:.2} chars", stats.std_dev)?;
    
    // Write 1.5 IQR threshold explanation
    writeln!(txt_file, "\nOUTLIER DETECTION THRESHOLD (1.5 × IQR method):")?;
    writeln!(txt_file, "Values above: {} chars may be considered outliers", outlier_threshold_upper as usize)?;
    writeln!(txt_file, "Values below: {} chars may be considered outliers (if positive)", 
             outlier_threshold_lower.max(0.0) as usize)?;
    
    // Write most frequent row lengths section with fixed column widths
    writeln!(txt_file, "\nCOMMON ROW LENGTHS")?;
    writeln!(txt_file, "{}", "-".repeat(100))?;
    writeln!(txt_file, "{:<15} {:<15} {:<15} {:<25} {:<25}", 
             "Row Length", "Count", "Percentage", "File Rows", "Data Indices")?;
    writeln!(txt_file, "{}", "-".repeat(100))?;
    
    // Convert HashMap to Vec for sorting by frequency
    let mut frequency_sorted: Vec<(usize, u64)> = length_counts.to_vec();
    // Sort by frequency (count) in descending order
    frequency_sorted.sort_by(|a, b| b.1.cmp(&a.1));
    
    // Display top 15 most common lengths by frequency
    let top_n = 15.min(frequency_sorted.len());
    for i in 0..top_n {
        let (length, count) = frequency_sorted[i];
        let percentage = (count as f64 / total_rows as f64) * 100.0;
        
        // Get example file rows for this length
        let file_rows = file_indices_map.get(&length)
            .map(|indices| {
                let max_examples = 3.min(indices.len());
                indices[0..max_examples].iter()
                    .map(|idx| idx.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_else(|| "N/A".to_string());
            
        // Get data indices for this length
        let data_indices = data_indices_map.get(&length)
            .map(|indices| {
                let max_examples = 3.min(indices.len());
                indices[0..max_examples].iter()
                    .map(|idx| idx.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_else(|| "N/A".to_string());
        
        writeln!(txt_file, "{:<15} {:<15} {:<15.2}% {:<25} {:<25}", 
                 length, count, percentage, file_rows, data_indices)?;
    }
    
    // Common Page Lengths Section
    writeln!(txt_file, "\nTOP 10 COMMON PAGE LENGTHS")?;
    writeln!(txt_file, "{}", "-".repeat(100))?;
    writeln!(txt_file, "{:<15} {:<15} {:<15} {:<25} {:<25}", 
             "Page Length", "Count", "Percentage", "File Rows", "Data Indices")?;
    writeln!(txt_file, "{}", "-".repeat(100))?;
    
    // Build map of page length to row indices
    let mut page_file_indices_map: HashMap<usize, Vec<usize>> = HashMap::new();
    let mut page_data_indices_map: HashMap<usize, Vec<isize>> = HashMap::new();
    
    // Populate the maps
    for (length, file_indices) in file_indices_map {
        let pages = (*length + CHARS_PER_PAGE - 1) / CHARS_PER_PAGE;
        for &file_idx in file_indices {
            page_file_indices_map.entry(pages)
                .or_insert_with(Vec::new)
                .push(file_idx);
        }
    }
    
    for (length, data_indices) in data_indices_map {
        let pages = (*length + CHARS_PER_PAGE - 1) / CHARS_PER_PAGE;
        for &data_idx in data_indices {
            page_data_indices_map.entry(pages)
                .or_insert_with(Vec::new)
                .push(data_idx);
        }
    }

    // Count frequencies
    let mut page_counts: HashMap<usize, u64> = HashMap::new();
    for (&page_len, indices) in &page_file_indices_map {
        page_counts.insert(page_len, indices.len() as u64);
    }

    // Convert to Vec for sorting by frequency
    let mut page_counts_vec: Vec<(usize, u64)> = page_counts.into_iter().collect();
    page_counts_vec.sort_by(|a, b| b.1.cmp(&a.1));

    // Display top 10 most common page lengths
    let top_n = 10.min(page_counts_vec.len());
    for i in 0..top_n {
        let (page_length, count) = page_counts_vec[i];
        let percentage = (count as f64 / total_rows as f64) * 100.0;
        
        // Get example file indices for this page length
        let file_indices = page_file_indices_map.get(&page_length)
            .map(|indices| {
                let max_examples = 3.min(indices.len());
                indices[0..max_examples].iter()
                    .map(|idx| idx.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_else(|| "N/A".to_string());
            
        // Get data indices for this page length
        let data_indices = page_data_indices_map.get(&page_length)
            .map(|indices| {
                let max_examples = 3.min(indices.len());
                indices[0..max_examples].iter()
                    .map(|idx| idx.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_else(|| "N/A".to_string());
        
        writeln!(txt_file, "{:<15} {:<15} {:<15.2}% {:<25} {:<25}", 
                page_length, count, percentage, file_indices, data_indices)?;
    }

    // Add explanatory note
    writeln!(txt_file, "\nNote: Page length is calculated using {} characters per page.", CHARS_PER_PAGE)?;
    
    // Extreme Values Section (largest rows)
    writeln!(txt_file, "\nEXTREME ROW LENGTHS (LARGEST ROWS)")?;
    writeln!(txt_file, "{}", "-".repeat(120))?;
    writeln!(txt_file, "{:<10} {:<15} {:<15} {:<15} {:<25} {:<25} {:<15}", 
             "Count", "Chars", "Words (est.)", "Pages (est.)", "File Rows", "Data Indices", "Std. Devs")?;
    writeln!(txt_file, "{}", "-".repeat(120))?;
    
    // Get the lengths sorted by size (descending)
    let mut lengths_by_size: Vec<usize> = length_counts.iter().map(|&(length, _)| length).collect();
    lengths_by_size.sort_by(|a, b| b.cmp(a));
    
    // Display top 20 largest rows
    let extreme_count = 20.min(lengths_by_size.len());
    for i in 0..extreme_count {
        let length = lengths_by_size[i];
        
        // Only process if we can find the count
        if let Some(count) = length_counts.iter().find(|&&(l, _)| l == length).map(|&(_, c)| c) {
            // Convert to estimated words and pages
            let words_est = length / 5;
            let pages_est = length as f64 / FLOAT_PAGE_SIZE;
            
            // Calculate standard deviations from mean
            let std_devs = (length as f64 - stats.mean).abs() / stats.std_dev;
            
            // Get file row indices for this length
            let file_rows = file_indices_map.get(&length)
                .map(|indices| {
                    let max_indices = 3.min(indices.len());
                    indices[0..max_indices].iter()
                        .map(|idx| idx.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_else(|| "N/A".to_string());
                
            // Get data indices for this length
            let data_indices = data_indices_map.get(&length)
                .map(|indices| {
                    let max_indices = 3.min(indices.len());
                    indices[0..max_indices].iter()
                        .map(|idx| idx.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_else(|| "N/A".to_string());
            
            writeln!(txt_file, "{:<10} {:<15} {:<15} {:<15.2} {:<25} {:<25} {:<15.2} σ", 
                     count, length, words_est, pages_est, file_rows, data_indices, std_devs)?;
        }
    }
    
    // Rows Above 1.5 IQR (Traditional Outliers)
    writeln!(txt_file, "\nROWS ABOVE 1.5 × IQR THRESHOLD")?;
    writeln!(txt_file, "{}", "-".repeat(100))?;
    writeln!(txt_file, "Any row length above {} characters is considered a statistical outlier.", 
             outlier_threshold_upper as usize)?;
    
    // Count outliers
    let outlier_lengths: Vec<usize> = lengths_by_size.iter()
        .filter(|&&length| (length as f64) > outlier_threshold_upper)
        .cloned()
        .collect();
    
    let total_outliers: u64 = outlier_lengths.iter()
        .filter_map(|&length| length_counts.iter().find(|&&(l, _)| l == length).map(|&(_, c)| c))
        .sum();
    
    writeln!(txt_file, "\nFound {} rows ({:.2}% of total) exceeding the outlier threshold.", 
             total_outliers, (total_outliers as f64 / total_rows as f64) * 100.0)?;
    
    if outlier_lengths.len() > 30 {
        writeln!(txt_file, "Showing the 30 largest outliers among {} different outlier lengths:", 
                 outlier_lengths.len())?;
    }
    
    // Table of outliers sorted by size
    writeln!(txt_file, "\n{:<15} {:<15} {:<25} {:<25} {:<15}", 
             "Row Length", "Count", "File Rows", "Data Indices", "Std. Deviations")?;
    writeln!(txt_file, "{}", "-".repeat(100))?;
    
    // Limit to 30 largest outliers
    let max_display = 30.min(outlier_lengths.len());
    for i in 0..max_display {
        let length = outlier_lengths[i];
        
        if let Some(count) = length_counts.iter().find(|&&(l, _)| l == length).map(|&(_, c)| c) {
            // Get file row indices for this length
            let file_rows = file_indices_map.get(&length)
                .map(|indices| {
                    let max_indices = 3.min(indices.len());
                    indices[0..max_indices].iter()
                        .map(|idx| idx.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_else(|| "N/A".to_string());
                
            // Get data indices for this length
            let data_indices = data_indices_map.get(&length)
                .map(|indices| {
                    let max_indices = 3.min(indices.len());
                    indices[0..max_indices].iter()
                        .map(|idx| idx.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_else(|| "N/A".to_string());
            
            // Calculate standard deviations from mean
            let std_devs = (length as f64 - stats.mean).abs() / stats.std_dev;
            
            writeln!(txt_file, "{:<15} {:<15} {:<25} {:<25} {:<15.2} σ", 
                     length, count, file_rows, data_indices, std_devs)?;
        }
    }
    
    // Recommendations section
    writeln!(txt_file, "\nRECOMMENDATIONS")?;
    writeln!(txt_file, "{}", "-".repeat(80))?;
    writeln!(txt_file, "Based on the analysis, here are some actionable recommendations:")?;
    
    // Address the extreme values
    if !lengths_by_size.is_empty() {
        let max_length = lengths_by_size[0];
        let max_page_est = max_length as f64 / FLOAT_PAGE_SIZE;
        
        writeln!(txt_file, "\nExtremely Large Rows:")?;
        writeln!(txt_file, "- The largest row contains {} characters (approximately {:.1} pages).", 
                 max_length, max_page_est)?;
        
        // Get the indices of the maximum length rows
        if let Some(indices) = file_indices_map.get(&max_length) {
            let max_indices = 5.min(indices.len());
            let indices_str = indices[0..max_indices].iter()
                .map(|idx| idx.to_string())
                .collect::<Vec<_>>()
                .join(", ");
                
            writeln!(txt_file, "- Investigate file rows: {}", indices_str)?;
            writeln!(txt_file, "- These rows are {:.2} standard deviations from the mean.", 
                     (max_length as f64 - stats.mean).abs() / stats.std_dev)?;
        }
        
        // Actionable advice
        writeln!(txt_file, "- Action: These rows may contain improperly formatted data or merged records.")?;
        writeln!(txt_file, "- Suggestion: Manually inspect these rows to determine if they need to be split or cleaned.")?;
    }
    
    // General recommendations based on distribution
    writeln!(txt_file, "\nGeneral Data Quality:")?;
    writeln!(txt_file, "- The median row length is {} characters.", stats.median)?;
    writeln!(txt_file, "- Rows with lengths near the median (between {} and {} characters) are likely to be properly formatted.", 
             stats.q1, stats.q3)?;
    
    // Special flags based on statistical properties
    if total_outliers > (total_rows / 10) {
        writeln!(txt_file, "- Warning: More than 10% of rows are statistical outliers, suggesting high variability in row structure.")?;
    }
    
    // Distribution shape information
    if stats.mean > (stats.median as f64) * 1.5 {
        writeln!(txt_file, "- The distribution is heavily skewed right (mean much larger than median), suggesting some extremely large values are affecting the average.")?;
    }
    
    // Explanation of indices
    writeln!(txt_file, "\nINDEX REFERENCE:")?;
    writeln!(txt_file, "- File Row: Physical line number in the file (1-based, starts at 1)")?;
    writeln!(txt_file, "- Data Index: Position in the data (-1 = header row, 0 = first data row, 1 = second data row, etc.)")?;
    writeln!(txt_file, "- For most use cases, you should refer to the File Row when locating rows in the original file")?;
    
    Ok(())
}

/// Generates a comprehensive markdown report with descriptive statistics and outlier identification.
/// 
/// # Arguments
/// 
/// * `report_path` - Path where the markdown report should be saved
/// * `basename` - Original filename basename for reporting
/// * `row_lengths` - Vector of all row lengths encountered
/// * `length_counts` - Vector of (length, count) pairs sorted by frequency
/// * `total_rows` - Total number of rows processed
/// * `total_chars` - Total number of characters across all rows
/// * `error_count` - Number of rows with reading errors
/// * `file_indices_map` - Map of row lengths to file row indices
/// * `data_indices_map` - Map of row lengths to data indices
/// 
/// # Returns
/// 
/// * `Result<(), io::Error>` - Ok(()) on success, or an Error if file operations fail
fn generate_markdown_outliers_report<P: AsRef<Path>>(
    report_path: P,
    basename: &str,
    row_lengths: &[usize],
    length_counts: &[(usize, u64)],
    total_rows: u64,
    total_chars: usize,
    error_count: u64,
    file_indices_map: &HashMap<usize, Vec<usize>>,
    data_indices_map: &HashMap<usize, Vec<isize>>,
) -> Result<(), io::Error> {
    let mut report_file = File::create(report_path)?;
    
    // Calculate descriptive statistics
    let stats = calculate_statistics(row_lengths);
    
    // Identify potential outliers - ensure all operands are f64
    let q1_f64 = stats.q1 as f64;
    let q3_f64 = stats.q3 as f64;
    let iqr = q3_f64 - q1_f64;
    let outlier_threshold_upper = q3_f64 + 1.5 * iqr;
    let outlier_threshold_lower = q1_f64 - 1.5 * iqr;
    
    // Write report header
    writeln!(report_file, "# Row Length Analysis for {}", basename)?;
    writeln!(report_file, "\nAnalysis performed on {} rows ({} with errors)", 
             total_rows, error_count)?;
    
    // Approx words and pages
    let estimated_words = total_chars / 5;  // Rough estimate: 5 chars per word on average
    let estimated_pages = total_chars / CHARS_PER_PAGE;  // Rough estimate: N chars per page
    
    // Write basic file statistics
    writeln!(report_file, "\n## File Statistics")?;
    writeln!(report_file, "- **Total Rows**: {}", total_rows)?;
    writeln!(report_file, "- **Total Characters**: {} (~{} words, ~{} pages)", 
             total_chars, estimated_words, estimated_pages)?;
    writeln!(report_file, "- **Average Characters Per Row**: {:.2} (~{:.1} words)", 
             total_chars as f64 / total_rows as f64, (total_chars as f64 / total_rows as f64) / 5.0)?;
    writeln!(report_file, "- **Unique Row Lengths**: {}", length_counts.len())?;
    
    // Write descriptive statistics section
    writeln!(report_file, "\n## Descriptive Statistics for Row Lengths")?;
    writeln!(report_file, "- **Minimum**: {} chars", stats.min)?;
    writeln!(report_file, "- **Maximum**: {} chars (~{} words, ~{:.1} pages)", 
             stats.max, stats.max / 5, stats.max as f64 / FLOAT_PAGE_SIZE)?;
    writeln!(report_file, "- **Range**: {} chars", stats.max - stats.min)?;
    writeln!(report_file, "- **Mean**: {:.2} chars", stats.mean)?;
    writeln!(report_file, "- **Median**: {} chars", stats.median)?;
    writeln!(report_file, "- **25th Percentile (Q1)**: {} chars", stats.q1)?;
    writeln!(report_file, "- **75th Percentile (Q3)**: {} chars", stats.q3)?;
    writeln!(report_file, "- **Interquartile Range (IQR)**: {} chars", stats.q3 - stats.q1)?;
    writeln!(report_file, "- **Standard Deviation**: {:.2} chars", stats.std_dev)?;
    
    // Write 1.5 IQR threshold explanation
    writeln!(report_file, "\n**Outlier Detection Threshold (1.5 × IQR method):**")?;
    writeln!(report_file, "- Values above: {} chars may be considered outliers", outlier_threshold_upper as usize)?;
    writeln!(report_file, "- Values below: {} chars may be considered outliers (if positive)", 
             outlier_threshold_lower.max(0.0) as usize)?;
    
    // Write most frequent row lengths section
    writeln!(report_file, "\n## Common Row Lengths")?;
    writeln!(report_file, "| Row Length | Count | Percentage | File Rows | Data Indices |")?;
    writeln!(report_file, "|------------|-------|------------|-----------|--------------|")?;
    
    // Convert HashMap to Vec for sorting by frequency
    let mut frequency_sorted: Vec<(usize, u64)> = length_counts.to_vec();
    // Sort by frequency (count) in descending order
    frequency_sorted.sort_by(|a, b| b.1.cmp(&a.1));
    
    // Display top 15 most common lengths by frequency
    let top_n = 15.min(frequency_sorted.len());
    for i in 0..top_n {
        let (length, count) = frequency_sorted[i];
        let percentage = (count as f64 / total_rows as f64) * 100.0;
        
        // Get example file rows for this length
        let file_rows = file_indices_map.get(&length)
            .map(|indices| {
                let max_examples = 3.min(indices.len());
                indices[0..max_examples].iter()
                    .map(|idx| idx.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_else(|| "N/A".to_string());
            
        // Get data indices for this length
        let data_indices = data_indices_map.get(&length)
            .map(|indices| {
                let max_examples = 3.min(indices.len());
                indices[0..max_examples].iter()
                    .map(|idx| idx.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_else(|| "N/A".to_string());
        
        writeln!(report_file, "| {} | {} | {:.2}% | {} | {} |", 
                length, count, percentage, file_rows, data_indices)?;
    }
        
    ////////////////////////////////
    // Common Page Lengths Section
    ////////////////////////////////
    
    // Build map of page length to row indices
    let mut page_file_indices_map: HashMap<usize, Vec<usize>> = HashMap::new();
    let mut page_data_indices_map: HashMap<usize, Vec<isize>> = HashMap::new();
    
    // Populate the maps
    for (length, file_indices) in file_indices_map {
        let pages = (*length + CHARS_PER_PAGE - 1) / CHARS_PER_PAGE;
        for &file_idx in file_indices {
            page_file_indices_map.entry(pages)
                .or_insert_with(Vec::new)
                .push(file_idx);
        }
    }
    
    for (length, data_indices) in data_indices_map {
        let pages = (*length + CHARS_PER_PAGE - 1) / CHARS_PER_PAGE;
        for &data_idx in data_indices {
            page_data_indices_map.entry(pages)
                .or_insert_with(Vec::new)
                .push(data_idx);
        }
    }

    // Count frequencies
    let mut page_counts: HashMap<usize, u64> = HashMap::new();
    for (&page_len, indices) in &page_file_indices_map {
        page_counts.insert(page_len, indices.len() as u64);
    }

    // Convert to Vec for sorting by frequency
    let mut page_counts_vec: Vec<(usize, u64)> = page_counts.into_iter().collect();
    page_counts_vec.sort_by(|a, b| b.1.cmp(&a.1));

    // Write Common Page Lengths section
    writeln!(report_file, "\n## Top 10 Common Page Lengths")?;
    writeln!(report_file, "| Page Length | Count | Percentage | File Rows | Data Indices |")?;
    writeln!(report_file, "|-------------|-------|------------|-----------|--------------|")?;

    // Display top 10 most common page lengths
    let top_n = 10.min(page_counts_vec.len());
    for i in 0..top_n {
        let (page_length, count) = page_counts_vec[i];
        let percentage = (count as f64 / total_rows as f64) * 100.0;
        
        // Get example file rows for this page length
        let file_rows = page_file_indices_map.get(&page_length)
            .map(|indices| {
                let max_examples = 3.min(indices.len());
                indices[0..max_examples].iter()
                    .map(|idx| idx.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_else(|| "N/A".to_string());
            
        // Get corresponding data_indices for this page length
        let data_indices = page_data_indices_map.get(&page_length)
            .map(|indices| {
                let max_examples = 3.min(indices.len());
                indices[0..max_examples].iter()
                    .map(|idx| idx.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_else(|| "N/A".to_string());
        
        writeln!(report_file, "| {} | {} | {:.2}% | {} | {} |", 
                page_length, count, percentage, file_rows, data_indices)?;
    }

    // Add explanatory note
    writeln!(report_file, "\n*Note: Page length is calculated using {} characters per page.*", CHARS_PER_PAGE)?;
            
    // Extreme Values Section (largest rows)
    writeln!(report_file, "\n## Extreme Row Lengths (Largest Rows)")?;
    writeln!(report_file, "| Count | Chars | Words (est.) | Pages (est.) | File Rows | Data Indices | Std. Devs from Mean |")?;
    writeln!(report_file, "|-------|-------|--------------|--------------|-----------|--------------|---------------------|")?;
    
    // Get the lengths sorted by size (descending)
    let mut lengths_by_size: Vec<usize> = length_counts.iter().map(|&(length, _)| length).collect();
    lengths_by_size.sort_by(|a, b| b.cmp(a));
    
    // Display top 20 largest rows
    let extreme_count = 20.min(lengths_by_size.len());
    for i in 0..extreme_count {
        let length = lengths_by_size[i];
        
        // Only process if we can find the count
        if let Some(count) = length_counts.iter().find(|&&(l, _)| l == length).map(|&(_, c)| c) {
            // Convert to estimated words and pages
            let words_est = length / 5;
            let pages_est = length as f64 / FLOAT_PAGE_SIZE;
            
            // Calculate standard deviations from mean
            let std_devs = (length as f64 - stats.mean).abs() / stats.std_dev;
            
            // Get file rows for this length
            let file_rows = file_indices_map.get(&length)
                .map(|indices| {
                    let max_indices = 3.min(indices.len());
                    indices[0..max_indices].iter()
                        .map(|idx| idx.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_else(|| "N/A".to_string());
                
            // Get data indices for this length
            let data_indices = data_indices_map.get(&length)
                .map(|indices| {
                    let max_indices = 3.min(indices.len());
                    indices[0..max_indices].iter()
                        .map(|idx| idx.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_else(|| "N/A".to_string());
            
            writeln!(report_file, "| {} | {} | {} | {:.2} | {} | {} | {:.2} σ |", 
                     count, length, words_est, pages_est, file_rows, data_indices, std_devs)?;
        }
    }
    
    // Rows Above 1.5 IQR (Traditional Outliers)
    writeln!(report_file, "\n## Rows Above 1.5 × IQR Threshold")?;
    writeln!(report_file, "Any row length above {} characters is considered a statistical outlier.", 
             outlier_threshold_upper as usize)?;
    
    // Count outliers
    let outlier_lengths: Vec<usize> = lengths_by_size.iter()
        .filter(|&&length| (length as f64) > outlier_threshold_upper)
        .cloned()
        .collect();
    
    let total_outliers: u64 = outlier_lengths.iter()
        .filter_map(|&length| length_counts.iter().find(|&&(l, _)| l == length).map(|&(_, c)| c))
        .sum();
    
    writeln!(report_file, "\nFound {} rows ({:.2}% of total) exceeding the outlier threshold.", 
             total_outliers, (total_outliers as f64 / total_rows as f64) * 100.0)?;
    
    if outlier_lengths.len() > 30 {
        writeln!(report_file, "Showing the 30 largest outliers among {} different outlier lengths:", 
                 outlier_lengths.len())?;
    }
    
    // Table of outliers sorted by size
    writeln!(report_file, "\n| Row Length | Count | File Rows | Data Indices | Standard Deviations |")?;
    writeln!(report_file, "|------------|-------|-----------|--------------|---------------------|")?;
    
    // Limit to 30 largest outliers
    let max_display = 30.min(outlier_lengths.len());
    for i in 0..max_display {
        let length = outlier_lengths[i];
        
        if let Some(count) = length_counts.iter().find(|&&(l, _)| l == length).map(|&(_, c)| c) {
            // Get file rows for this length
            let file_rows = file_indices_map.get(&length)
                .map(|indices| {
                    let max_indices = 3.min(indices.len());
                    indices[0..max_indices].iter()
                        .map(|idx| idx.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_else(|| "N/A".to_string());
                
            // Get data indices for this length
            let data_indices = data_indices_map.get(&length)
                .map(|indices| {
                    let max_indices = 3.min(indices.len());
                    indices[0..max_indices].iter()
                        .map(|idx| idx.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_else(|| "N/A".to_string());
            
            // Calculate standard deviations from mean
            let std_devs = (length as f64 - stats.mean).abs() / stats.std_dev;
            
            writeln!(report_file, "| {} | {} | {} | {} | {:.2} σ |", 
                     length, count, file_rows, data_indices, std_devs)?;
        }
    }
    
    // Recommendations section
    writeln!(report_file, "\n## Recommendations")?;
    writeln!(report_file, "Based on the analysis, here are some actionable recommendations:")?;
    
    // Address the extreme values
    if !lengths_by_size.is_empty() {
        let max_length = lengths_by_size[0];
        let max_page_est = max_length as f64 / FLOAT_PAGE_SIZE;
        
        writeln!(report_file, "\n### Extremely Large Rows")?;
        writeln!(report_file, "- The largest row contains {} characters (approximately {:.1} pages).", 
                 max_length, max_page_est)?;
        
        // Get the indices of the maximum length rows
        if let Some(indices) = file_indices_map.get(&max_length) {
            let max_indices = 5.min(indices.len());
            let indices_str = indices[0..max_indices].iter()
                .map(|idx| idx.to_string())
                .collect::<Vec<_>>()
                .join(", ");
                
            writeln!(report_file, "- Investigate file rows: {}", indices_str)?;
            writeln!(report_file, "- These rows are {:.2} standard deviations from the mean.", 
                     (max_length as f64 - stats.mean).abs() / stats.std_dev)?;
        }
        
        // Actionable advice
        writeln!(report_file, "- **Action**: These rows may contain improperly formatted data or merged records.")?;
        writeln!(report_file, "- **Suggestion**: Manually inspect these rows to determine if they need to be split or cleaned.")?;
    }
    
    // General recommendations based on distribution
    writeln!(report_file, "\n### General Data Quality")?;
    writeln!(report_file, "- The median row length is {} characters.", stats.median)?;
    writeln!(report_file, "- Rows with lengths near the median (between {} and {} characters) are likely to be properly formatted.", 
             stats.q1, stats.q3)?;
    
    // Special flags based on statistical properties
    if total_outliers > (total_rows / 10) {
        writeln!(report_file, "- **Warning**: More than 10% of rows are statistical outliers, suggesting high variability in row structure.")?;
    }
    
    // Distribution shape information
    if stats.mean > (stats.median as f64) * 1.5 {
        writeln!(report_file, "- The distribution is heavily skewed right (mean much larger than median), suggesting some extremely large values are affecting the average.")?;
    }
    
    // Index explanation
    writeln!(report_file, "\n## Index Reference")?;
    writeln!(report_file, "- **File Row**: Physical line number in the file (1-based, starts at 1)")?;
    writeln!(report_file, "- **Data Index**: Position in the data (-1 = header row, 0 = first data row, 1 = second data row, etc.)")?;
    writeln!(report_file, "- For most use cases, you should refer to the File Row when locating rows in the original file")?;
    
    Ok(())
}

/// A structure to hold descriptive statistics
struct Statistics {
    min: usize,
    max: usize,
    mean: f64,
    median: usize,
    q1: usize,
    q3: usize,
    std_dev: f64,
}

/// Calculate descriptive statistics for a set of row lengths
/// 
/// # Arguments
/// 
/// * `lengths` - Vector of row lengths to analyze
/// 
/// # Returns
/// 
/// * `Statistics` - Calculated statistics
fn calculate_statistics(lengths: &[usize]) -> Statistics {
    if lengths.is_empty() {
        return Statistics {
            min: 0,
            max: 0,
            mean: 0.0,
            median: 0,
            q1: 0,
            q3: 0,
            std_dev: 0.0,
        };
    }
    
    // Create a sorted copy for quantile calculations
    let mut sorted = lengths.to_vec();
    sorted.sort();
    
    let len = sorted.len();
    let min = *sorted.first().unwrap_or(&0);
    let max = *sorted.last().unwrap_or(&0);
    
    // Calculate mean
    let sum: usize = sorted.iter().sum();
    let mean = sum as f64 / len as f64;
    
    // Calculate median and quartiles
    let median = if len % 2 == 0 {
        (sorted[len/2 - 1] + sorted[len/2]) / 2
    } else {
        sorted[len/2]
    };
    
    // Calculate Q1 (25th percentile)
    let q1_idx = len / 4;
    let q1 = if len % 4 == 0 {
        (sorted[q1_idx - 1] + sorted[q1_idx]) / 2
    } else {
        sorted[q1_idx]
    };
    
    // Calculate Q3 (75th percentile)
    let q3_idx = (3 * len) / 4;
    let q3 = if (3 * len) % 4 == 0 {
        (sorted[q3_idx - 1] + sorted[q3_idx]) / 2
    } else {
        sorted[q3_idx]
    };
    
    // Calculate standard deviation
    let variance: f64 = sorted.iter()
        .map(|&x| {
            let diff = x as f64 - mean;
            diff * diff
        })
        .sum::<f64>() / len as f64;
    
    let std_dev = variance.sqrt();
    
    Statistics {
        min,
        max,
        mean,
        median,
        q1,
        q3,
        std_dev,
    }
}

/// Extracts the basename from a file path without extension.
/// 
/// # Arguments
/// 
/// * `file_path` - The file path to extract basename from
/// 
/// # Returns
/// 
/// * `Result<String, io::Error>` - The basename without extension or an error
fn extract_basename(file_path: impl AsRef<Path>) -> Result<String, io::Error> {
    let path_ref = file_path.as_ref();
    
    // Get the filename
    let filename = path_ref.file_name()
        .ok_or_else(|| io::Error::new(
            io::ErrorKind::InvalidInput, 
            format!("Invalid file path: {:?}", path_ref)
        ))?;
    
    // Convert to string and remove extension
    let filename_str = filename.to_string_lossy();
    Ok(filename_str
        .split('.')
        .next()
        .unwrap_or("unknown")
        .to_string())
}

/// Generates a timestamp string for unique filenames.
/// 
/// # Returns
/// 
/// * `Result<String, io::Error>` - Timestamp string or error if system time cannot be accessed
fn generate_timestamp() -> Result<String, io::Error> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    
    Ok(format!("{}", duration.as_secs()))
}

/// Parses command line arguments into input file/directory and output directory.
/// 
/// # Arguments
/// 
/// * `args` - Command line arguments vector
/// 
/// # Returns
/// 
/// * `Result<(InputSource, String), String>` - Tuple of (input_source, output_dir) or error message
fn parse_arguments(args: &[String]) -> Result<(InputSource, String), String> {
    if args.len() < 2 {
        return Err("Missing input argument. Use a file path or --directory <path>".to_string());
    }
    
    let mut output_dir = "reports".to_string();
    let mut input_source = InputSource::SingleFile(String::new());
    let mut i = 1;
    
    while i < args.len() {
        match args[i].as_str() {
            "--directory" => {
                if i + 1 < args.len() {
                    input_source = InputSource::Directory(args[i + 1].clone());
                    i += 2;
                } else {
                    return Err("--directory requires a path argument".to_string());
                }
            },
            arg if i == 1 && !arg.starts_with("--") => {
                // First argument is a file path
                input_source = InputSource::SingleFile(arg.to_string());
                i += 1;
            },
            arg if arg.starts_with("--") => {
                return Err(format!("Unknown argument: {}", arg));
            },
            _ => {
                // Non-flag argument after first must be output directory
                output_dir = args[i].clone();
                i += 1;
            }
        }
    }
    
    // Validate input source
    match &input_source {
        InputSource::SingleFile(path) => {
            if path.is_empty() {
                return Err("Missing input file path".to_string());
            }
        },
        InputSource::Directory(path) => {
            if path.is_empty() {
                return Err("Missing directory path".to_string());
            }
        }
    }
    
    Ok((input_source, output_dir))
}

/// Process all CSV files in a directory and generate analysis reports for each.
/// 
/// # Arguments
/// 
/// * `directory_path` - Path to the directory containing CSV files to analyze
/// * `output_directory` - Directory where all report files will be saved
/// 
/// # Returns
/// 
/// * `Result<usize, io::Error>` - Number of successfully processed files or an I/O error
fn process_directory(
    directory_path: impl AsRef<Path>, 
    output_directory: impl AsRef<Path>
) -> Result<usize, io::Error> {
    let mut processed_count = 0;
    
    for entry in fs::read_dir(directory_path)? {
        let entry = entry?;
        let path = entry.path();
        
        // Check if it's a CSV file
        if path.is_file() {
            if let Some(extension) = path.extension() {
                if extension.to_string_lossy().to_lowercase() == "csv" {
                    // Extract basename for display
                    let basename = path.file_name()
                        .and_then(|n| n.to_str())
                        .unwrap_or("unknown");
                    
                    println!("Processing CSV file: {}", basename);
                    
                    // Process the CSV file - Convert to String for type compatibility
                    let path_str = path.to_string_lossy().to_string();
                    let output_dir_str = output_directory.as_ref().to_string_lossy().to_string();
                    
                    match analyze_csv_row_lengths(path_str, output_dir_str) {
                        Ok(_) => {
                            processed_count += 1;
                            print_success_message(basename);
                        },
                        Err(e) => {
                            eprintln!("Error analyzing CSV file {}: {}", basename, e);
                            // Continue with other files even if one fails
                        }
                    }
                }
            }
        }
    }
    
    Ok(processed_count)
}

/// Print success message after processing a CSV file
/// 
/// # Arguments
/// 
/// * `basename` - Base name of the processed file
fn print_success_message(basename: &str) {
    println!("Generated six report files with prefix '{}_':", basename);
    println!("  1. {}_char_counts_report_*.csv\n   - Contains file_row, data_index, and character count for each row", basename);
    println!("  2. {}_value_counts_report_*.csv\n   - Contains frequency distribution of row lengths (sorted by count)", basename);
    println!("  3. {}_md_outliers_report_*.md\n   - Contains descriptive statistics and potential outliers", basename);
    println!("  4. {}_txt_outliers_report_*.txt\n   - Plain text version of outliers report with evenly spaced columns", basename);
    println!("  5. {}_pages_valuecounts_report_*.csv\n   - Contains distribution of rows by page length ({} chars per page)", 
        basename, CHARS_PER_PAGE);
    println!("  6. {}_length_sorted_report_*.csv\n   - Contains file_row, data_index, and character count for each row (sorted by length descending)", basename);
    println!("\nIndex Explanation:");
    println!("  - file_row: Physical line number in the file (1-based, starts at 1)");
    println!("  - data_index: Position in the data (-1 = header row, 0 = first data row, 1 = second data row, etc.)");
    println!("  When examining the original file, always use file_row to locate specific rows");
    println!();
}

/// Main entry point for the CSV row character-count analyzer application.
pub fn csv_row_analyzer_parallel_main() {
    // Get command line arguments
    let args: Vec<String> = env::args().collect();
    
    // Parse arguments or use defaults
    let (input_source, output_dir) = parse_arguments(&args).unwrap_or_else(|err| {
        eprintln!("Error parsing arguments: {}", err);
        eprintln!("Usage: {} <input_csv_path> [output_directory]", args[0]);
        eprintln!("   or: {} --directory <directory_path> [output_directory]", args[0]);
        eprintln!("Example: {} large_dataset.csv ./my_reports", args[0]);
        eprintln!("Example: {} --directory ./csv_files ./my_reports", args[0]);
        process::exit(1);
    });
    
    match input_source {
        InputSource::SingleFile(input_file) => {
            // Extract basename for display
            let basename = Path::new(&input_file)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("unknown");
            
            println!("Analyzing CSV file: {} ({})", basename, input_file);
            println!("Reports will be saved to: {}", output_dir);
            
            // Process the CSV file
            if let Err(e) = analyze_csv_row_lengths(&input_file, &output_dir) {
                eprintln!("Error analyzing CSV file: {}", e);
                process::exit(1);
            }
            
            print_success_message(basename);
        },
        InputSource::Directory(dir_path) => {
            println!("Analyzing all CSV files in directory: {}", dir_path);
            println!("Reports will be saved to: {}", output_dir);
            
            // Process all CSV files in directory
            match process_directory(&dir_path, &output_dir) {
                Ok(file_count) => {
                    println!("Successfully processed {} CSV files from directory", file_count);
                },
                Err(e) => {
                    eprintln!("Error processing directory: {}", e);
                    process::exit(1);
                }
            }
        }
    }
}