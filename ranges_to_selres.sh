#!/bin/bash

# Log file for errors
log_file="error_log.txt"
# Clear the log file at the start
: > "$log_file"

# Function to process CSV file
process_csv() {
  local csv_file=$1

  echo "Processing CSV file: $csv_file"

  # Read each row from the CSV file
  while IFS=',' read -r name region; do
    # Replace ':' with '_' in region
    valid_region=${region//:/_}

    # Check if the PDB file exists
    if [[ ! -f "$name.pdb" ]]; then
      echo "Error: PDB file not found: $name.pdb" >> "$log_file"
      continue
    fi

    # Check if both the PDB and FASTA files already exist
    if [[ -f "${name}-${valid_region}.pdb" && -f "${name}-${valid_region}.fasta" ]]; then
      echo "Skipping: $name, Region: $region (already processed)"
      continue
    fi

    echo "Processing: $name, Region: $region"

    # Execute pdb_selres and write directly to the final PDB file
    if pdb_selres "-$region" < "$name.pdb" > "${name}-${valid_region}.pdb"; then
      # Convert the PDB output to FASTA format and write to the final FASTA file
      if pdb_tofasta < "${name}-${valid_region}.pdb" > "${name}-${valid_region}.fasta"; then
        echo "Processed successfully: $name, Region: $region"
      else
        echo "Error: Failed to convert to FASTA format: $name, Region: $region" >> "$log_file"
      fi
    else
      echo "Error: Failed to execute pdb_selres: $name, Region: $region" >> "$log_file"
    fi

  done < "$csv_file"
}

# Export function for parallel processing
export -f process_csv

# Process CSV files in parallel using 7 cores with progress bar and ETA
find . -name '*_range.csv' | parallel -j 7 --bar --eta process_csv

# Print completion message
echo "Processing completed."
