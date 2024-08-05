#!/bin/bash

# Replace 'example.csv' with your actual CSV file name
csv_file='example'

# Log file for storing error messages
log_file='fetch_error_log.txt'

# Define the number of parallel processes
num_processes=$(nproc)  # Number of CPU cores

# Function to fetch and process data for a each pdb ID in csv file
fetch_and_process() {
    local name="$1"
    if [ -f "$name.pdb" ]; then
        echo "Skipping $name. PDB file already exists."
        return
    fi
    
    #helix ranges created for each PDB ID
    if pdb_fetch "$name" | tee "$name.pdb" | 
    awk -v name="$name" '{if ($1 == "HELIX" && $10 == "1") {print name","$6":"$9 > name"_range.csv"}}'; then
        echo "Data fetched successfully for: $name"  
    else
        echo "Failed to fetch data for: $name" >> "$log_file"
    fi
}

# Export the function to make it available to parallel
export -f fetch_and_process

# Process PDB ID in parallel with progress monitoring
cat "$csv_file" | parallel -j num_processes --bar --eta fetch_and_process {}

# Print completion message
echo "Processing completed."
