import os
import pandas as pd

def combine_csv_files(input_folder):
    """
    Reads all CSV files in a specified folder, combines them into a single DataFrame,
    and saves the combined data to a new CSV file in the same folder.

    Args:
        input_folder (str): The path to the folder containing the CSV files.
    """
    all_files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]
    
    if not all_files:
        print(f"No CSV files found in {input_folder}")
        return

    df_list = []
    for file in all_files:
        file_path = os.path.join(input_folder, file)
        try:
            df = pd.read_csv(file_path)
            df_list.append(df)
        except Exception as e:
            print(f"Error reading {file}: {e}")
            continue

    if not df_list:
        print("No CSV files were successfully read.")
        return

    combined_df = pd.concat(df_list, ignore_index=True)
    output_file_path = os.path.join(input_folder, 'combined_output.csv')
    
    try:
        combined_df.to_csv(output_file_path, index=False)
        print(f"Combined CSV saved to: {output_file_path}")
    except Exception as e:
        print(f"Error saving combined CSV: {e}")



def extract_and_combine_column(input_folder, column_name):
    """
    Reads all CSV files in a specified folder, extracts a single specified column
    from each, combines the extracted data into a single DataFrame, and saves
    the combined data to a new CSV file in the same folder.

    Args:
        input_folder (str): The path to the folder containing the CSV files.
        column_name (str): The name of the column to extract from each CSV file.
    """
    all_files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]
    
    if not all_files:
        print(f"No CSV files found in {input_folder}")
        return

    extracted_data_list = []
    for file in all_files:
        file_path = os.path.join(input_folder, file)
        try:
            df = pd.read_csv(file_path, dtype={column_name: str})
            if column_name in df.columns:
                extracted_data_list.append(df[[column_name]])
            else:
                print(f"Column '{column_name}' not found in {file}. Skipping this file.")
        except Exception as e:
            print(f"Error reading {file}: {e}")
            continue

    if not extracted_data_list:
        print(f"No data extracted for column '{column_name}' from any CSV file.")
        return

    combined_column_df = pd.concat(extracted_data_list, ignore_index=True)
    output_file_path = os.path.join(input_folder, f'combined_{column_name}.csv')
    
    try:
        combined_column_df.to_csv(output_file_path, index=False)
        print(f"Combined '{column_name}' column saved to: {output_file_path}")
    except Exception as e:
        print(f"Error saving combined '{column_name}' CSV: {e}")


def extract_column_with_filter(input_folder, extract_column, filter_column, filter_values):
    """
    Reads all CSV files in a specified folder, extracts the specified column
    only for rows where another column matches any of the provided filter values,
    combines the extracted data into a single DataFrame, and saves the combined
    data to a new CSV file in the same folder.

    Args:
        input_folder (str): The path to the folder containing the CSV files.
        extract_column (str): The name of the column to extract.
        filter_column (str): The column to filter on.
        filter_values (list): List of values to match in the filter_column.
    """
    all_files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]
    if not all_files:
        print(f"No CSV files found in {input_folder}")
        return

    extracted_data_list = []
    for file in all_files:
        file_path = os.path.join(input_folder, file)
        try:
            df = pd.read_csv(file_path, dtype={extract_column: str, filter_column: str})
            if extract_column in df.columns and filter_column in df.columns:
                filtered = df[df[filter_column].isin(filter_values)]
                if not filtered.empty:
                    extracted_data_list.append(filtered[[extract_column]])
            else:
                print(f"Required columns not found in {file}. Skipping this file.")
        except Exception as e:
            print(f"Error reading {file}: {e}")
            continue

    if not extracted_data_list:
        print(f"No data extracted for column '{extract_column}' with filter on '{filter_column}'.")
        return

    combined_filtered_df = pd.concat(extracted_data_list, ignore_index=True)
    output_file_path = os.path.join(
        input_folder,
        f'combined_{extract_column}_filtered_by_{filter_column}.csv'
    )
    try:
        combined_filtered_df.to_csv(output_file_path, index=False)
        print(f"Filtered '{extract_column}' column saved to: {output_file_path}")
    except Exception as e:
        print(f"Error saving filtered CSV: {e}")

if __name__ == "__main__":
    #combine_csv_files("test_csv_folder")
    #extract_and_combine_column("ein_data", "EIN")
    extract_column_with_filter("ein_data", "EIN", "STATE", ["MI", "GA"])