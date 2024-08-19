# Code for ETL operations on Country-GDP data

# Importing the required libraries
import datetime, pandas as pd, numpy as np, requests, sqlite3
from bs4 import BeautifulSoup
from io import StringIO
def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing.'''
    
    # Get the current timestamp
    time_stamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Format the log message
    log_message = f"{time_stamp} : {message}\n"
    
    # Write the log message to the log file
    with open("code_log.txt", "a") as log_file:
        log_file.write(log_message)

def extract(url, heading):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    
    # Send a GET request to the URL
    response = requests.get(url)
    
    # Parse the page content using BeautifulSoup
    soup = BeautifulSoup(response.content, "html.parser")
    
    # Find the heading (e.g., "By market capitalization")
    heading_tag = soup.find("span", {"id": heading}).parent
    
    # Find the table immediately following this heading
    table = heading_tag.find_next("table")
    
    # Convert the table HTML to a string
    table_html = str(table)
    
    # Use StringIO to wrap the HTML string
    table_io = StringIO(table_html)
    
    # Read the HTML table into a DataFrame
    df = pd.read_html(table_io)[0]
    
    # Clean the 'Market cap' column
    df['Market cap (US$ billion)'] = df['Market cap (US$ billion)'].replace({'\n': ''}, regex=True).astype(float)
    
    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    # Read the exchange rate CSV file into a DataFrame
    exchange_rate_df = pd.read_csv(csv_path)
    
    # Convert the DataFrame to a dictionary
    exchange_rate = exchange_rate_df.set_index('Currency')['Rate'].to_dict()
    
    # Add the three new columns to the DataFrame
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['Market cap (US$ billion)']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['Market cap (US$ billion)']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['Market cap (US$ billion)']]
    
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

    df.to_csv(output_path, index=False)
    log_progress("Data saved to CSV file")

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_progress("Data loaded to Database as a table, Executing queries")

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    cursor = sql_connection.cursor()
    print(f"Executing Query: {query_statement}")
    cursor.execute(query_statement)
    result = cursor.fetchall()
    for row in result:
        print(row)
    log_progress(f"Query executed: {query_statement}")

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''
# Example of making the first log entry
log_progress("Preliminaries complete. Initiating ETL process")
# URL of the webpage
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"

# Call the extract function and print the resulting DataFrame
extracted_df = extract(url, "By_market_capitalization")
print(extracted_df)

# Log the completion of data extraction
log_progress("Data extraction complete. Initiating Transformation process")
# Example call to transform() function
csv_path = 'exchange_rate.csv'  # Replace with the actual path to your exchange rate CSV file
transformed_df = transform(extracted_df, csv_path)

# Log the completion of data transformation
log_progress("Data transformation complete. Initiating Loading process")

# Print the contents of df['MC_EUR_Billion'][4]
print(transformed_df['MC_EUR_Billion'][4])

# Save the final datafram as a CSV file
load_to_csv(transformed_df, "banks_project.csv")
print("Data saved to CSV successfully.")

# Assuming df is your transformed DataFrame
db_name = "Banks.db"  # Database name
table_name = "Largest_banks"  # Table name

# Initiate SQLite3 connection
conn = sqlite3.connect(db_name)
log_progress("SQL Connection initiated")

# Load the data to the database
load_to_db(transformed_df, conn, table_name)

# Confirm the process completion
print("Data loaded to Database successfully.")

# Close the database connection
conn.close()
log_progress("Server Connection closed")

# Run queries on Database
db_name = "Banks.db"  # Database name
conn = sqlite3.connect(db_name)
log_progress("SQL Connection initiated")

# Query 1: Print the contents of the entire table
query_1 = "SELECT * FROM Largest_banks"
run_query(query_1, conn)

# Query 2: Print the average market capitalization in GBP
query_2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_2, conn)

# Query 3: Print only the names of the top 5 banks
query_3 = "SELECT `Bank name` FROM Largest_banks LIMIT 5"
run_query(query_3, conn)

# Close the database connection
conn.close()
log_progress("Server Connection closed")