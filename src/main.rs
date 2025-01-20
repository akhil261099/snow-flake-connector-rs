// use snowflake_connector_rs::{SnowflakeClient, SnowflakeAuthMethod, SnowflakeClientConfig, SnowflakeSession};
// use std::io;
// use std::fs::File;
// use csv::ReaderBuilder;
// use std::time::{Duration, Instant};
// use std::env;

// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     // Get command-line arguments
//     let args: Vec<String> = env::args().collect();

//     // Ensure the user and password are provided
//     if args.len() < 3 {
//         eprintln!("Usage: cargo run <user> <password> <command>");
//         return Err("Insufficient arguments".into());
//     }
//     // let user = "akhil969".to_string();
//     // let password = "zaq1ZAQ1@1".to_string();
//     let user = args[1].clone();
//     let password = args[2].clone();
//     // let account = "kcb57939.us-east-1".to_string();
//     let account = "GMGNHXO-WM55675".to_string();
//     // let account = args[3].clone();
//     let role = Some("ACCOUNTADMIN".to_string());
//     let warehouse = Some("COMPUTE_WH".to_string());
//     let database = Some("TRAININGDB".to_string());
//     let schema = Some("SALES".to_string());
//     let timeout = 60; // Timeout in seconds (e.g., 60 seconds)

//     // Initialize Snowflake client
//     let client = SnowflakeClient::new(
//         &user,
//         SnowflakeAuthMethod::Password(password),
//         SnowflakeClientConfig {
//             account,
//             role,
//             warehouse,
//             database,
//             schema,
//             timeout: Some(Duration::from_secs(timeout)),
//         },
//     )?;
//     let session = client.create_session().await?;

//     // Check command-line argument for the desired operation
//     // if args.len() < 3 {
//     //     eprintln!("You must specify a command (either 'upload_csv_to_snowflake' or 'execute_select_query')");
//     //     return Err("Missing command argument".into());
//     // }

//     let command = &args[3];

//     match command.as_str() {
//         "create" => upload_csv_to_snowflake(&session).await?,
//         "query" => execute_select_query(&session).await?,
//         _ => {
//             eprintln!("Invalid command: {}", command);
//             return Err("Invalid command".into());
//         }
//     }

//     Ok(())
// }

// async fn upload_csv_to_snowflake(session: &SnowflakeSession) -> Result<(), Box<dyn std::error::Error>> {
//     let start = Instant::now();
//     // Create a table in Snowflake
//     // println!("Enter your create_table Query");
//     // CREATE OR REPLACE TABLE MAIN_TABLE (SepalLength NUMBER(3,2),SepalWidth NUMBER(3,2),PetalLength NUMBER(3,2),PetalWidth NUMBER(3,2),Species STRING);
//     let mut create_table_sql = String::new();
//     io::stdin().read_line(&mut create_table_sql)?;

//     session.execute(create_table_sql).await?;
//     println!("Table created successfully.");

//     // File path to the CSV file
//     let file_path = r"E:\snowflake-connector-rs\connector\src\iris_dataset.csv";
//     let file = File::open(file_path)?;

//     // Read CSV file and insert records into the Snowflake table
//     let mut rdr = ReaderBuilder::new().has_headers(false).from_reader(file);

//     // INSERT INTO MAIN_TABLE (SepalLength,SepalWidth,PetalLength,PetalWidth,Species)
//     println!("Enter the Insert Query");
//     let mut insert_query: String = String::new();
//     io::stdin().read_line(&mut insert_query)?;
//     let insert_query = insert_query.trim();

//     for result in rdr.records() {
//         let record = result?;
//         let row: Vec<String> = record.iter().map(|field| field.to_string()).collect();

//         // Format the row values and execute the INSERT query
//         let values_str = row.iter()
//             .map(|field| format!("'{}'", field.replace("'", "''")))
//             .collect::<Vec<String>>()
//             .join(", ");

//         let insert_sql = format!("{} VALUES ({})", insert_query, values_str);
//         session.execute(insert_sql).await?;
//     }
//     let duration = start.elapsed();
//     println!("Time taken to execute the upload_csv Query is :{:?}", duration);
//     println!("CSV file uploaded successfully.");
//     Ok(())
// }

// async fn execute_select_query(session: &SnowflakeSession) -> Result<(), Box<dyn std::error::Error>> {
//     let start = Instant::now();

//     // Prompt user for a SELECT query
//     let mut query = String::new();
//     // println!("Enter the query:");
//     io::stdin().read_line(&mut query)?;

//     // Execute the query
//     let result = session.execute(query.trim()).await?;

//     // Fetch all rows (adjust according to your session.execute() return type)
//     let rows = result.fetch_all().await?;

//     // Ensure we have some rows
//     if rows.is_empty() {
//         println!("No results found.");
//         return Ok(());
//     }

//     // Print column names as the header row
//     let column_names: Vec<String> = rows[0].column_names().iter().map(|&name| name.to_string()).collect();
//     println!("{}", column_names.join(","));

//     // Iterate through the rows and print values
//     for row in rows {
//         let values: Vec<String> = column_names.iter()
//             .map(|column_name| {
//                 // Try to fetch each value and handle NULLs gracefully
//                 row.get::<String>(column_name).unwrap_or_else(|_| "NULL".to_string())
//             })
//             .collect();
        
//         // Print the row's values as a CSV row
//         println!("{}", values.join(","));
//     }

//     // Measure time taken for the query execution
//     let duration = start.elapsed();
//     println!("Time taken for execution is: {:?}", duration);
//     println!("Query executed successfully");

//     Ok(())
// }
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
