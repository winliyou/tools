use std::collections::HashMap;
use std::process::{Command, ExitCode};

#[derive(Debug)]
enum ShortStatParseStage {
    ParseCommitId,
    ParseAuthor,
    ParseUpdateLines,
}

fn main() -> Result<ExitCode, ExitCode> {
    let opts = clap::Command::new(file!())
        .author("Liyou")
        .version("0.0.1")
        .about("generate git statistic by commit count , insert count ,delete count")
        .arg_required_else_help(true)
        .arg(
            clap::Arg::new("directory")
                .short('d')
                .long("directory")
                .default_value(".")
                .help("source directory to generate statistic"),
        )
        .arg(
            clap::Arg::new("branch")
                .short('b')
                .long("branch")
                .default_value("master")
                .help("branch to generate statistic"),
        )
        .arg(
            clap::Arg::new("batch_count")
                .short('n')
                .long("batch_count")
                .default_value("200")
                .help("how many commit should be paresed one time"),
        )
        .get_matches();

    let branch: String = opts.get_one::<String>("branch").unwrap().clone();
    let directory: String = opts.get_one::<String>("directory").unwrap().clone();
    let batch_count_str = opts.get_one::<String>("batch_count").unwrap();
    let mut batch_count = 200;
    if let Ok(t) = batch_count_str.parse::<usize>() {
        batch_count = t;
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(8)
        .enable_all()
        .build()
        .unwrap();
    // Get total commit count in the specified directory for specified branch
    let total_commit_count_output = Command::new("git")
        .args(["-C", &format!("{}", directory)])
        .args(&["rev-list", "--count"])
        .arg(format!("refs/heads/{}", branch))
        .output()
        .expect("Failed to execute git command");
    if !total_commit_count_output.status.success() {
        eprintln!(
            "Failed to get commit count for branch {} and directory {}",
            branch, directory
        );
        return Err(ExitCode::FAILURE);
    }

    let mut total_commit_count = 0;
    let total_commit_count_output_str = String::from_utf8_lossy(&total_commit_count_output.stdout);
    for line in total_commit_count_output_str.lines() {
        if let Ok(count) = line.parse::<usize>() {
            total_commit_count += count;
        }
    }

    // Initialize a HashMap to store author stats
    let author_stats: std::sync::Arc<std::sync::Mutex<HashMap<String, (usize, usize, usize)>>> =
        std::sync::Arc::new(std::sync::Mutex::new(HashMap::new()));

    let partical_stats_vec: std::sync::Arc<
        std::sync::Mutex<Vec<HashMap<String, (usize, usize, usize)>>>,
    > = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    // Iterate through all commit and calculate commit count and total changes (insertions + deletions)
    let mut worker_vec = std::vec![];
    for commit_index in (0..total_commit_count).step_by(batch_count) {
        let branch = branch.clone();
        let directory = directory.clone();
        let partical_stats_vec_clone = partical_stats_vec.clone();
        let statistic_worker = rt.spawn(async move {
            let shortstat_output = Command::new("git")
                .args(["-C", &format!("{}", directory)])
                .args(&["log", "--shortstat"])
                .arg(format!("--skip={}", commit_index))
                .args(["-n", &format!("{}", batch_count)])
                .arg(format!("refs/heads/{}", branch))
                .output()
                .expect("Failed to execute git command");
            let shortstat_output_str = String::from_utf8_lossy(&shortstat_output.stdout);

            let mut partical_author_stats: HashMap<String, (usize, usize, usize)> = HashMap::new();

            let mut current_commit_id = String::new();
            let mut commit_author = String::new();
            let mut current_parse_stage: ShortStatParseStage = ShortStatParseStage::ParseCommitId;

            for shortstat_output_line in shortstat_output_str.lines() {
                match current_parse_stage {
                    ShortStatParseStage::ParseCommitId => {
                        let (issuccessful, tmp_commit_id) = get_commit_id(shortstat_output_line);
                        if issuccessful {
                            current_commit_id = tmp_commit_id;
                            current_parse_stage = ShortStatParseStage::ParseAuthor;
                        }
                    }
                    ShortStatParseStage::ParseAuthor => {
                        let (issuccessful, tmp_commit_author) =
                            get_commit_author(shortstat_output_line);
                        if issuccessful {
                            commit_author = tmp_commit_author;
                            current_parse_stage = ShortStatParseStage::ParseUpdateLines;
                        }
                    }
                    ShortStatParseStage::ParseUpdateLines => {
                        let (issuccessful, (commit_line_insert, commit_line_delete)) =
                            get_commit_file_change(shortstat_output_line);
                        if issuccessful {
                            current_parse_stage = ShortStatParseStage::ParseCommitId;

                            if !current_commit_id.is_empty() {
                                let commit_author_stats =
                                    partical_author_stats.get_mut(&commit_author);
                                if let Some((commit_count, line_insert, line_delete)) =
                                    commit_author_stats
                                {
                                    *commit_count += 1;
                                    *line_insert += commit_line_insert;
                                    *line_delete += commit_line_delete;
                                } else {
                                    partical_author_stats.insert(
                                        commit_author.clone(),
                                        (1, commit_line_insert, commit_line_delete),
                                    );
                                }
                            }
                        }
                    }
                }
            }
            partical_stats_vec_clone
                .lock()
                .unwrap()
                .push(partical_author_stats);
        });
        worker_vec.push(statistic_worker);
    }

    rt.block_on(async {
        for a in worker_vec {
            match tokio::join!(a) {
                _ => (),
            }
        }
    });

    for partical_data in partical_stats_vec.lock().unwrap().iter() {
        let mut author_stats_data = author_stats.lock().unwrap();
        for (k, v) in partical_data.iter() {
            if let Some((commit_count, line_insert, line_delete)) = author_stats_data.get_mut(k) {
                *commit_count += v.0;
                *line_insert += v.1;
                *line_delete += v.2;
            } else {
                author_stats_data.insert(k.to_string(), *v);
            }
        }
    }
    // Sort authors by commit count
    let start_time = std::time::Instant::now();

    if let Ok(author_commit_stats) = author_stats.lock() {
        let mut sorted_authors_by_commits = author_commit_stats
            .iter()
            .collect::<Vec<(&String, &(usize, usize, usize))>>();
        sorted_authors_by_commits.sort_by_key(|&(_, (commit_count, _, _))| commit_count);
        sorted_authors_by_commits.reverse();

        // Print rankings by commit count
        println!("Rankings by Commit Count:");
        for (rank, (author, (commit_count, _, _))) in sorted_authors_by_commits.iter().enumerate() {
            println!("Rank {}: {} - {} commits", rank + 1, author, commit_count);
        }
    } else {
        println!("No authors found");
    };

    let end_time = start_time.elapsed();
    println!("time used: sort by commit: {}ms", end_time.as_millis());

    // Sort authors by line insert changes
    let start_time = std::time::Instant::now();
    if let Ok(author_line_insert_stats) = author_stats.lock() {
        let mut sorted_authors_by_insert_changes = author_line_insert_stats
            .iter()
            .collect::<Vec<(&String, &(usize, usize, usize))>>();
        sorted_authors_by_insert_changes.sort_by_key(|&(_, (_, line_insert, _))| line_insert);
        sorted_authors_by_insert_changes.reverse();
        // Print rankings by insert changes
        println!("\nRankings by Insert Changes:");
        for (rank, (author, (_, line_insert, _))) in
            sorted_authors_by_insert_changes.iter().enumerate()
        {
            println!(
                "Rank {}: {} - {} line insert changes",
                rank + 1,
                author,
                line_insert
            );
        }
    } else {
        println!("Error: Failed to acquire lock on author_stats");
    }
    let end_time = start_time.elapsed();
    println!("time used: sort by insert: {}ms", end_time.as_millis());

    // Sort authors by line delete changes
    let start_time = std::time::Instant::now();

    if let Ok(author_line_delete_stats) = author_stats.lock() {
        let mut sorted_authors_by_delete_changes = author_line_delete_stats
            .iter()
            .collect::<Vec<(&String, &(usize, usize, usize))>>();
        sorted_authors_by_delete_changes.sort_by_key(|&(_, (_, _, line_delete))| line_delete);
        sorted_authors_by_delete_changes.reverse();
        // Print rankings by delete changes
        println!("\nRankings by Delete Changes:");
        for (rank, (author, (_, _, line_delete))) in
            sorted_authors_by_delete_changes.iter().enumerate()
        {
            println!(
                "Rank {}: {} - {} line delete changes",
                rank + 1,
                author,
                line_delete
            );
        }
    } else {
        println!("Error: Failed to acquire lock on author_line_stats");
    }
    let end_time = start_time.elapsed();
    println!("time used: sort by delete: {}ms", end_time.as_millis());

    Ok(ExitCode::SUCCESS)
}

fn get_commit_id(test_str: &str) -> (bool, String) {
    let mut is_successful: bool = false;
    let mut commit_id_str = String::new();
    let commit_id_regex = regex::Regex::new(r"^commit (?<commit_id>[0-9a-f]{40})").unwrap();

    if let Some(change_item) = commit_id_regex.captures(test_str) {
        if let Some(commit_id_item) = change_item.name("commit_id") {
            commit_id_str = commit_id_item.as_str().to_string();
            is_successful = true;
        }
    }
    (is_successful, commit_id_str)
}

fn get_commit_author(test_str: &str) -> (bool, String) {
    let mut is_successful: bool = false;
    let mut author_str = String::new();
    let commit_author_regex = regex::Regex::new(r"^Author: (?<author_name>[^<]+) <.+").unwrap();

    if let Some(change_item) = commit_author_regex.captures(test_str) {
        if let Some(author_item) = change_item.name("author_name") {
            author_str = author_item.as_str().to_string();
            is_successful = true;
        }
    }
    (is_successful, author_str)
}

fn get_commit_file_change(test_str: &str) -> (bool, (usize, usize)) {
    let mut is_successful: bool = false;
    let mut insert_change = 0;
    let mut delete_change = 0;
    let file_changes_regex = regex::Regex::new(
    r"(?<file_count>\d+) files? changed(, (?<insert_count>\d+) insertions?\(\+\))?(, (?<delete_count>\d+) deletions?\(-\))?"
)
.unwrap();
    if let Some(change_item) = file_changes_regex.captures(test_str) {
        if let Some(insert_count_item) = change_item.name("insert_count") {
            if let Ok(insert_count) = insert_count_item.as_str().parse::<usize>() {
                insert_change += insert_count;
                is_successful = true;
            }
        }
        if let Some(delete_count_item) = change_item.name("delete_count") {
            if let Ok(delete_count) = delete_count_item.as_str().parse::<usize>() {
                delete_change += delete_count;
                is_successful = true;
            }
        }
    }
    (is_successful, (insert_change, delete_change))
}
