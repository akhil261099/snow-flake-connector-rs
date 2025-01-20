use snowflake_connector_rs::{SnowflakeClient, SnowflakeAuthMethod, SnowflakeClientConfig, SnowflakeSession};
use std::time::{Duration};
use std::env;
use tokio;

mod upload_csv_to_table;
mod execute;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Get command-line arguments
    let args: Vec<String> = env::args().collect();

    // Ensure the user and password are provided
    if args.len() < 3 {
        eprintln!("Usage: cargo run <user> <password> <command>");
        return Err("Insufficient arguments".into());
    }

    let user = args[1].clone();
    let password = args[2].clone();
    let account = "GMGNHXO-WM55675".to_string();
    let role = Some("ACCOUNTADMIN".to_string());
    let warehouse = Some("COMPUTE_WH".to_string());
    let database = Some("TRAININGDB".to_string());
    let schema = Some("SALES".to_string());
    let timeout = 60; // Timeout in seconds (e.g., 60 seconds)

    // Initialize Snowflake client
    let client = SnowflakeClient::new(
        &user,
        SnowflakeAuthMethod::Password(password),
        SnowflakeClientConfig {
            account,
            role,
            warehouse,
            database,
            schema,
            timeout: Some(Duration::from_secs(timeout)),
        },
    )?;
    let session = client.create_session().await?;

    // Check command-line argument for the desired operation
    let command = &args[3];

    match command.as_str() {
        "create" => upload_csv_to_table::upload_csv_to_snowflake(&session).await?,
        "query" => execute::execute_select_query(&session).await?,
        _ => {
            eprintln!("Invalid command: {}", command);
            return Err("Invalid command".into());
        }
    }

    Ok(())
}
