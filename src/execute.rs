use snowflake_connector_rs::SnowflakeSession;
use std::io;
use std::time::Instant;

pub async fn execute_select_query(session: &SnowflakeSession) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();

    // Prompt user for a SELECT query
    let mut query = String::new();
    io::stdin().read_line(&mut query)?;

    // Execute the query
    let result = session.execute(query.trim()).await?;

    // Fetch all rows (adjust according to your session.execute() return type)
    let rows = result.fetch_all().await?;

    // Ensure we have some rows
    if rows.is_empty() {
        println!("No results found.");
        return Ok(());
    }

    // Print column names as the header row
    let column_names: Vec<String> = rows[0].column_names().iter().map(|&name| name.to_string()).collect();
    println!("{}", column_names.join(","));

    // Iterate through the rows and print values
    for row in rows {
        let values: Vec<String> = column_names.iter()
            .map(|column_name| {
                // Try to fetch each value and handle NULLs gracefully
                row.get::<String>(column_name).unwrap_or_else(|_| "NULL".to_string())
            })
            .collect();
        
        // Print the row's values as a CSV row
        println!("{}", values.join(","));
    }

    // Measure time taken for the query execution
    let duration = start.elapsed();
    println!("Time taken for execution is: {:?}", duration);
    println!("Query executed successfully");

    Ok(())
}
