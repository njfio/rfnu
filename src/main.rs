mod neo4j_client;

use log::{debug, info, error};
use env_logger::Env;
use std::process::{Command, Stdio};
use std::io::{Write, BufRead, BufReader};
use std::env;
use serde_json::json;
use thiserror::Error;
use tokio::process::Command as TokioCommand;
use neo4j_client::{Neo4jClient, Neo4jClientError};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct Node {
    id: String,
    content: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct SimilarPair {
    start_id: String,
    end_id: String,
    similarity: f64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct KeywordPair {
    start_id: String,
    end_id: String,
    keywords: Vec<String>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct CausalPair {
    id: String,
    phrase: String,
    context: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct HierarchicalPair {
    id: String,
    heading: String,
}

#[derive(Error, Debug)]
pub enum MainError {
    #[error("Neo4j client error: {0}")]
    Neo4jClientError(#[from] Neo4jClientError),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Python script error: {0}")]
    PythonScriptError(String),
    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),
}

#[tokio::main]
async fn main() -> Result<(), MainError> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    // Print current working directory for debugging
    let current_dir = env::current_dir()?;
    println!("Current working directory: {:?}", current_dir);

    let client = Neo4jClient::new("bolt://localhost:7687", "neo4j", "system2024!", "neo4j").await?;

    info!("Querying nodes...");
    let nodes = client.query_nodes().await?;
    let node_data: Vec<_> = nodes.iter().map(|node| {
        json!({
            "id": node.entity.replace("Node(", "").replace(")", ""),
            "content": node.properties.iter().find(|prop| prop.name == "content").map(|prop| &prop.value).unwrap_or(&String::new())
        })
    }).collect();

    let node_data_json = serde_json::to_string(&node_data)?;
    debug!("Node data JSON: {}", node_data_json);

    info!("Running vector analysis...");

    // Provide the absolute path to the Python script
    let script_path = "/Users/n/RustroverProjects/rfnu/src/vectorize_and_analyze.py";
    debug!("Script path: {:?}", script_path);

    if !std::path::Path::new(script_path).exists() {
        return Err(MainError::IoError(std::io::Error::new(std::io::ErrorKind::NotFound, "Python script not found")));
    }

    let input_file_path = "/Users/n/RustroverProjects/rfnu/temp_input.json";
    let output_file_path = "/Users/n/RustroverProjects/rfnu/temp_output.json";

    // Write node data to a temporary input file
    std::fs::write(input_file_path, node_data_json)?;

    let mut child = Command::new("python3")
        .arg(script_path)
        .arg(input_file_path)
        .arg(output_file_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to run vectorize_and_analyze.py");

    let stdout = BufReader::new(child.stdout.take().expect("Failed to capture stdout"));
    let stderr = BufReader::new(child.stderr.take().expect("Failed to capture stderr"));

    // Capture and print stdout and stderr in real-time
    let stdout_thread = std::thread::spawn(move || {
        let mut output = String::new();
        for line in stdout.lines() {
            if let Ok(line) = line {
                println!("STDOUT: {}", line);
                output.push_str(&line);
                output.push('\n');
            }
        }
        output
    });

    let stderr_thread = std::thread::spawn(move || {
        for line in stderr.lines() {
            if let Ok(line) = line {
                eprintln!("STDERR: {}", line);
            }
        }
    });

    stdout_thread.join().expect("Failed to join stdout thread");
    stderr_thread.join().expect("Failed to join stderr thread");

    debug!("Finished running vector analysis");

    if !child.wait()?.success() {
        return Err(MainError::PythonScriptError("Python script failed".into()));
    }

    // Read the output file from the Python script
    let output_data = std::fs::read_to_string(output_file_path)?;
    let result: serde_json::Value = serde_json::from_str(&output_data)?;

    // Process the output from the Python script
    let similar_pairs: Vec<SimilarPair> = serde_json::from_value(result["similar_pairs"].clone())?;
    let keyword_pairs: Vec<KeywordPair> = serde_json::from_value(result["keyword_pairs"].clone())?;
    let causal_pairs: Vec<CausalPair> = serde_json::from_value(result["causal_pairs"].clone())?;
    let hierarchical_pairs: Vec<HierarchicalPair> = serde_json::from_value(result["hierarchical_pairs"].clone())?;

    info!("Creating new relationships...");
    for pair in similar_pairs {
        if let Err(e) = client.create_relationship(&pair.start_id, &pair.end_id, "SIMILAR_TO").await {
            error!("Failed to create relationship between {} and {}: {:?}", pair.start_id, pair.end_id, e);
        }
    }

    for pair in keyword_pairs {
        if let Err(e) = client.create_relationship(&pair.start_id, &pair.end_id, "KEYWORD_OVERLAP").await {
            error!("Failed to create relationship between {} and {}: {:?}", pair.start_id, pair.end_id, e);
        }
    }

    for pair in causal_pairs {
        debug!("Creating relationship for causal pair: {}", pair.id);
        if let Some(node_id) = client.get_node_id_by_content(&pair.context).await? {
            if let Err(e) = client.create_relationship(&node_id, &pair.id, "CAUSED_BY").await {
                error!("Failed to create causal relationship for node {}: {:?}", pair.id, e);
            }
        }
    }

    for pair in hierarchical_pairs {
        debug!("Creating relationship for hierarchical pair: {}", pair.id);
        if let Some(node_id) = client.get_node_id_by_content(&pair.heading).await? {
            if let Err(e) = client.create_relationship(&node_id, &pair.id, "PART_OF").await {
                error!("Failed to create hierarchical relationship for node {}: {:?}", pair.id, e);
            }
        }
    }

    info!("Done");

    Ok(())
}
