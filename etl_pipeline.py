import pandas as pd  # Importing pandas for working with tabular data

# STEP 1: EXTRACT
def extract_data(file_path):
    """
    Reads a CSV file into a pandas DataFrame.
    """
    df = pd.read_csv(file_path)
    print("✅ Extracted Data:")
    print(df)
    return df

# STEP 2: TRANSFORM
def transform_data(df):
    """
    Cleans the data:
    - Removes rows where department or salary is missing
    - Fills missing ages with the average age
    - Converts salary to integer type
    """
    df = df.dropna(subset=['department', 'salary'])  # remove rows with missing critical data
    df['age'].fillna(df['age'].mean(), inplace=True)  # fill missing ages with average
    df['salary'] = df['salary'].astype(int)  # convert salary to int
    print("✅ Transformed Data:")
    print(df)
    return df

# STEP 3: LOAD
def load_data(df, output_file_path):
    """
    Saves the cleaned DataFrame to a CSV file.
    """
    df.to_csv(output_file_path, index=False)
    print(f"✅ Cleaned data saved to {output_file_path}")

# MAIN FUNCTION
def main():
    """
    Controls the flow: Extract ➝ Transform ➝ Load
    """
    input_path = 'input/employees.csv'
    output_path = 'output/cleaned_employees.csv'

    df = extract_data(input_path)
    cleaned_df = transform_data(df)
    load_data(cleaned_df, output_path)

# Run the pipeline
if __name__ == '__main__':
    main()