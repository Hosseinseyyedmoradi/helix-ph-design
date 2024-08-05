import os
import pandas as pd
from Bio import SeqIO

def fasta_files_to_dataframe(directory):
    sequences = []

    # Iterate through all .fasta files in the directory
    for filename in os.listdir(directory):
        if filename.endswith(".fasta"):
            filepath = os.path.join(directory, filename)
            for record in SeqIO.parse(filepath, "fasta"):
                sequences.append({
                    "id": filename,  # Use filename as ID
                    "description": record.description,
                    "sequence": str(record.seq)
                })

    # Convert list of dictionaries to DataFrame
    df = pd.DataFrame(sequences)
    return df

# Replace 'your_directory' with the path to your directory containing the FASTA files
directory = os.path.expanduser('~/Downloads/pepfeature_test')
df = fasta_files_to_dataframe(directory)

# Print the resulting DataFrame
print(df)
