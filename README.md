# ETL Pipeline for Customer Transactions

This project implements an ETL (Extract, Transform, Load) pipeline to process and clean customer transaction data. The pipeline uses Python, Pandas, and SQLite to manage data effectively, focusing on enhancing data quality and enabling better analytics.

## Project Overview

The ETL pipeline performs the following operations:

1. **Extract**: Simulates the extraction of raw customer transaction data, including various data quality issues such as missing values and duplicates.
2. **Transform**: Applies a series of data cleaning and transformation techniques, including:
   - Handling missing values
   - Removing duplicates
   - Converting data types (e.g., date parsing)
   - Detecting and handling outliers
   - String manipulation and standardization
   - Data validation
3. **Load**: Loads the cleaned data into an SQLite database for further analysis.

## Requirements

To run this project, you will need:

- Python 3.x
- Pandas library
- SQLite3 (included with Python)
- ConfigParser (included with Python)

You can install the required Python packages using pip:

```bash
pip install pandas
```

**Step 1: Extract**
Simulates the extraction of raw customer transaction data.
```python
def extract():
    logging.info("Extracting data...")
    return raw_df
```

**Step 2: Transform**
Applies data cleaning and transformation techniques.
```python

def transform(df):
    logging.info("Transforming data...")

    # Drop rows with missing customer_id
    df = df.dropna(subset=['customer_id'])

    # Fill missing 'amount_spent' with the mean value
    df['amount_spent'] = df['amount_spent'].fillna(df['amount_spent'].mean())

    # Fill missing 'product_category' with 'Unknown'
    df['product_category'] = df['product_category'].fillna('Unknown')

    # Fill missing transaction dates with a placeholder
    df['transaction_date'] = df['transaction_date'].fillna('2024-01-01')

    # Remove duplicates
    df = df.drop_duplicates()

    # Convert transaction_date to datetime format
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])

    # Detect and handle outliers in 'amount_spent' using IQR
    Q1 = df['amount_spent'].quantile(0.25)
    Q3 = df['amount_spent'].quantile(0.75)
    IQR = Q3 - Q1
    df = df[~((df['amount_spent'] < (Q1 - 1.5 * IQR)) | (df['amount_spent'] > (Q3 + 1.5 * IQR)))]

    # Standardize customer names (e.g., title case)
    df['customer_name'] = df['customer_name'].str.title()

    # Reset index after transformation
    df = df.reset_index(drop=True)

    return df
```

**Functionality**: 

Drop Rows: Removes rows with missing customer_id.
Fill Missing Values: Fills missing amount_spent with the mean value, product_category with 'Unknown', and transaction dates with a placeholder.
Remove Duplicates: Ensures each transaction is unique.
Convert Data Types: Converts transaction_date to a datetime format for easier manipulation.
Outlier Detection: Uses the Interquartile Range (IQR) method to identify and remove outliers in the amount_spent column.
String Manipulation: Standardizes the case of customer_name to title case.
Reset Index: Resets the index of the DataFrame after transformations.

**Step 3: Load**
Loads the cleaned data into an SQLite database.

```python
def load(df):
    logging.info("Loading data into the database...")
    # Create an SQLite connection
    conn = sqlite3.connect('etl_pipeline_project.db')
    df.to_sql('transactions', conn, if_exists='replace', index=False)
    conn.close()
```

  **Functionality**: 
Creates an SQLite connection to etl_pipeline_project.db.
Loads the cleaned DataFrame into a table named transactions.
Closes the database connection after loading.

**ETL Pipeline Execution**
Coordinates the ETL process by calling the extract, transform, and load functions.
```python
def etl_pipeline():
    # Extract
    extracted_data = extract()

    # Transform
    transformed_data = transform(extracted_data)

    # Load
    load(transformed_data)

    logging.info("ETL pipeline executed successfully.")
    return transformed_data
```

**Functionality**: 
Calls the extract(), transform(), and load() functions sequentially to execute the full ETL process.
Logs a success message upon completion.

**Main Execution Block**
Executes the ETL pipeline when the script is run.
```python
if __name__ == "__main__":
    etl_pipeline()
```
**Functionality**: Ensures that the ETL pipeline runs when the script is executed directly.

# Advanced ETL Pipeline for Customer Transactions

This repository contains an advanced ETL pipeline designed for processing customer transaction data, focusing on data quality and integrity.

## Key Features

- **Robust Data Cleaning**: The pipeline implements advanced data cleaning techniques, ensuring high data quality for analytics.
- **Logging**: Integrated logging to track the ETL process and identify issues efficiently.
- **Data Validation**: Ensures the integrity of the data before loading it into the database.

## Example of Data Cleaning Techniques

- **Handling Missing Values**:
  - Fills missing `amount_spent` with the mean of the column.
  - Fills missing `product_category` with `'Unknown'`.
  - Fills missing `transaction_date` with a placeholder date.

- **Outlier Detection**:
  - Uses the Interquartile Range (IQR) method to detect and remove outliers in the `amount_spent` column.

- **String Manipulation**:
  - Strips leading and trailing spaces and standardizes string cases to improve consistency in data.

## Techniques Used

- **Pandas**: Utilized for data manipulation and cleaning operations within the pipeline.
- **SQLite**: Used for loading the cleaned data into a relational database.
- **Logging Module**: Integrated to provide insights into the ETL process and facilitate debugging.

## How to Use

You can run the ETL pipeline by executing the Python script in any environment that supports Python and has the necessary libraries (such as Pandas and SQLite) installed. 

## Contact

Feel free to connect with me for any inquiries or collaboration opportunities.


