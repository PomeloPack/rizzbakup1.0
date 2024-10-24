#!/bin/bash

# Define the paths to your scripts
script1="/home/mholomek/check_clearing.py"
script2="/home/mholomek/check_tapregistry.py"

log_file="/home/mholomek/script_errors.log"


# Run the first script and capture output and errors
output1=$(python3.9 $script1 2>&1)
status1=$?

# Print the output and errors of the first script
echo "Output of the first script:"
echo "$output1"

# Check if the first script ran successfully
if [ $status1 -ne 0 ]; then
  echo "First script failed with status $status1"
  echo "Error details: $output1" >> $log_file
  echo "Error details have been logged to $log_file"
  exit 1
fi

# Wait for 5 seconds
#sleep 5

# Run the second script and capture output and errors
output2=$(python3.9 $script2 2>&1)
status2=$?

# Print the output and errors of the second script
echo "Output of the second script:"
echo "$output2"

# Check if the second script ran successfully
if [ $status2 -ne 0 ]; then
  echo "Second script failed with status $status2"
  echo "Error details: $output2" >> $log_file
  echo "Error details have been logged to $log_file"
  exit 1
fi
