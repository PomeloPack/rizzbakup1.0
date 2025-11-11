#!/bin/bash

# Define the paths to your scripts
script1="/home/mholomek/check_clearing.py"
script2="/home/mholomek/check_tapregistry.py"



# Run the first script
python3.9 $script1

# Check if the first script ran successfully
if [ $? -ne 0 ]; then
  echo "First script failed"
  exit 1
fi

echo " "
echo " "
echo "------------------------------------------------------------------------------------"
echo " "
echo " "

sleep 1

# Run the second script
python3.9 $script2

# Check if the second script ran successfully
if [ $? -ne 0 ]; then
  echo "Second script failed"
  exit 1
fi