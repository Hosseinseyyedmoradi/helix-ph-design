import os
import glob
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import subprocess
import tempfile

def fetch_fasta_sequence(pdb_file):
    """Generate the complete FASTA sequence from a PDB file."""
    try:
        result = subprocess.run(f"pdb_tofasta {pdb_file}", shell=True, capture_output=True, text=True, check=True)
        complete_fasta = result.stdout.strip()
        return ''.join(line.strip() for line in complete_fasta.splitlines() if not line.startswith('>'))
    except subprocess.CalledProcessError as e:
        print(f"Error generating FASTA sequence for {pdb_file}: {e}")
        return ''

def process_pdb_file(pdb_file):
    """Process a single PDB file to extract helix/sheet sequences and compute the unstructured sequence."""
    pdb_id = os.path.splitext(os.path.basename(pdb_file))[0]
    structured_data = []
    ranges = []
    
    total_fasta = fetch_fasta_sequence(pdb_file)

    try:
        with open(pdb_file, 'r') as file:
            for line in file:
                if line.startswith("HELIX") or line.startswith("SHEET"):
                    parts = line.split()
                    region_type = "H" if line.startswith("HELIX") else "B"
                    start = int(parts[5]) if region_type == "H" else int(parts[6])
                    end = int(parts[8]) if region_type == "H" else int(parts[9])
                    
                    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdb") as temp_pdb:
                        temp_pdb_path = temp_pdb.name

                    try:
                        pdb_selres = f"pdb_selres -{start}:{end} {pdb_file} > {temp_pdb_path}"
                        subprocess.run(pdb_selres, shell=True, check=True)

                        temp_to_fasta = f"pdb_tofasta {temp_pdb_path}"
                        result = subprocess.run(temp_to_fasta, shell=True, capture_output=True, text=True)
                        structured_sequence = ''.join(line.strip() for line in result.stdout.strip().splitlines() if not line.startswith('>'))

                        structured_data.append([pdb_id, region_type, start, end, structured_sequence])
                        ranges.append((start, end))
                    finally:
                        if os.path.exists(temp_pdb_path):
                            os.remove(temp_pdb_path)
    except Exception as e:
        print(f"Error processing file {pdb_file}: {e}")

    unstructured_sequences = []
    prev_end = 0
    for start, end in sorted(ranges):
        unstructured_sequences.append(total_fasta[prev_end:start-1])
        prev_end = end

    unstructured_sequences.append(total_fasta[prev_end:])
    unstructured_fasta = '|'.join(unstructured_sequences)

    return structured_data, unstructured_fasta

def load_processed_files(log_file):
    """Load the list of already processed PDB files from the log file."""
    if os.path.exists(log_file):
        processed_files_df = pd.read_csv(log_file)
        return set(processed_files_df['PDB_ID'].tolist())
    return set()

def save_processed_files(log_file, processed_files):
    """Save the list of processed PDB files to the log file."""
    processed_files_df = pd.DataFrame(list(processed_files), columns=['PDB_ID'])
    processed_files_df.to_csv(log_file, index=False)

def process_all_pdb_files():
    """Process all PDB files in the directory and save results to CSV files."""
    log_file = 'processed_files_log.csv'
    processed_files = load_processed_files(log_file)
    
    pdb_files = glob.glob("*.pdb")
    structured_data = []
    unstructured_fasta_dict = {}

    # Filter files that need to be processed
    to_process_files = [f for f in pdb_files if os.path.splitext(os.path.basename(f))[0] not in processed_files]

    print(f"Total PDB files to process: {len(to_process_files)}")
    
    if not to_process_files:
        print("All PDB files have been processed.")
        return
    
    with ProcessPoolExecutor(max_workers=7) as executor:
        futures = {executor.submit(process_pdb_file, pdb_file): pdb_file for pdb_file in to_process_files}

        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing PDB files"):
            pdb_file = futures[future]
            pdb_id = os.path.splitext(os.path.basename(pdb_file))[0]
            if pdb_id in processed_files:
                print(f"File already processed: {pdb_id}")
                continue

            print(f"Processing PDB file: {pdb_id}")
            try:
                result, unstructured_fasta = future.result()
                structured_data.extend(result)
                if result:
                    unstructured_fasta_dict[pdb_id] = unstructured_fasta
                    processed_files.add(pdb_id)
                    print(f"Processed PDB file: {pdb_id}")
            except Exception as e:
                print(f"Error processing file {pdb_file}: {e}")

    save_processed_files(log_file, processed_files)

    structured_data_df = pd.DataFrame(structured_data, columns=["PDB_ID", "Type", "Start", "End", "FASTA"])
    unstructured_fasta_df = pd.DataFrame(list(unstructured_fasta_dict.items()), columns=["PDB_ID", "Unstructured_FASTA"])
    
    print("Structured Ranges:")
    print(structured_data_df)
    print("Unstructured Sequences:")
    print(unstructured_fasta_df)

    structured_data_df.to_csv("structured_ranges.csv", index=False)
    unstructured_fasta_df.to_csv("unstructured_sequences.csv", index=False)

    print("All PDB files have been processed.")

if __name__ == "__main__":
    process_all_pdb_files()
