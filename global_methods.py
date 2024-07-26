import yaml
from urllib.parse import quote, unquote
import json
import os
import csv


def load_yaml_config(file_path):
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config

def get_text_from_file(file_path): 
    with open(file_path, 'r') as file:
        # Read the entire content of the file
        content = file.read()

    return content






# ====================================================================================================
# Checkpoint Functions
# ====================================================================================================
import json
import os

CHECKPOINT_FILE = 'checkpoint.json'

def save_checkpoint(search_term_index, cleaned_papers_list, concept_edges_list):
    checkpoint = {
        'search_term_index': search_term_index,
        'cleaned_papers': cleaned_papers_list,
        'concept_edges': concept_edges_list
    }
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f)

def load_checkpoint(start_term_input):
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            checkpoint = json.load(f)
            return checkpoint['search_term_index'], checkpoint['cleaned_papers'], checkpoint['concept_edges']
    else:
        return start_term_input, [], []  # Start from the beginning if no checkpoint exists





# ====================================================================================================
# CSV Processing Functions
# ====================================================================================================

import pandas as pd

def save_to_csv(df, file_name, folder_name):
    file_path = './resources/' + folder_name + '/' + file_name + '.csv'
    df.to_csv(file_path, index=False)
    print(f"DataFrame saved to {file_path}")

def load_from_csv(file_name, folder_name):
    try:
        file_path = './resources/' + folder_name + '/' + file_name + '.csv'
        df = pd.read_csv(file_path)
        print(f"DataFrame loaded from {file_path}")
        return df
    except Exception as e:
        return None
    
import csv

def load_from_csv_non_pandas(file_name, folder_name):
    try:
        file_path = './resources/' + folder_name + '/' + file_name + '.csv'
        data = []
        with open(file_path, newline='') as csvfile:
            csvreader = csv.reader(csvfile)
            headers = next(csvreader)  # Skip the header row if present
            for row in csvreader:
                data.append(row)
        print(f"Data loaded from {file_path}")
        return data
    except Exception as e:
        return None



def load_dataframe_from_list(data, column_names):
    """
    Load a pandas DataFrame from a list of lists.

    Parameters:
    data (list of lists): The data to load into the DataFrame.
    column_names (list of str): The names of the columns for the DataFrame.

    Returns:
    pandas.DataFrame: The created DataFrame.
    """
    # Ensure the data structure is correct
    for row in data:
        if len(row) != len(column_names):
            raise ValueError("Each row in the data must have the same number of elements as there are column names.")
    
    df = pd.DataFrame(data, columns=column_names)
    return df