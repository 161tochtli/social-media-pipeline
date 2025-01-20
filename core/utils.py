import subprocess
import pandas as pd
import os
import json

def merge_csv_files(files, output):
    """
    Merges multiple CSV files into a single CSV file using a Bash script.
    Keeps the header from the first file and removes headers from all subsequent files.

    :param files: A list of Path objects pointing to the CSV files to merge.
    :param output: A Path object pointing to the output CSV file.
    """

    # Convert Path objects to strings for the command line
    files_str = ' '.join([str(file) for file in files])
    output_str = str(output)

    # Bash command to merge CSV files
    # Note: Using 'echo' to handle the first file's header, then 'tail' to skip headers for the rest
    cmd = f'''
    output="{output_str}"
    > "$output" # Ensure the output file is empty or create it if it doesn't exist
    for file in {files_str}; do
        if [ "$file" == "{files[0]}" ]; then
            head -n 1 "$file" > "$output" # Add header from the first file
        fi
        tail -n +2 "$file" >> "$output" # Skip the header and append
    done
    '''

    # Execute the Bash command
    subprocess.run(cmd, shell=True, check=True, executable='/bin/bash')


def drop_duplicates(csv_path):
    # drop duplicates
    df = pd.read_csv(csv_path)
    n = len(df)
    df = df.drop_duplicates()
    m = len(df)
    print(f"Se quitaron {n-m} registros duplicados")
    df.to_csv(csv_path, index=False)

def load_config():
    with open("config.json", "r", encoding="utf-8") as f:
        data = json.load(f)
    return json.loads(json.dumps(data))#, object_hook=lambda d: SimpleNamespace(**d))