#!/bin/bash

# List of endpoints
urls=(
    "http://localhost:4151/api/management/mgmt/tap/send/BKK"
    "http://localhost:4151/api/management/mgmt/tap/send/LARISSA"
    "http://localhost:4151/api/management/mgmt/tap/send/SCHWEINFURT"
    "http://localhost:4151/api/management/mgmt/tap/send/EFE_VALPARAISO"
    "http://localhost:4151/api/management/mgmt/tap/send/TGZM"
    "http://localhost:4151/api/management/mgmt/tap/send/KAUNAS"
    "http://localhost:4151/api/management/mgmt/tap/send/BISTRITA"
    "http://localhost:4151/api/management/mgmt/tap/send/DSZO"
)

# Loop through URLs and send curl requests
for url in "${urls[@]}"; do
    echo "Sending request to: $url"
    curl -u mgmt:mgmt -X GET "$url"
    echo " - Curl request completed. Waiting 3 seconds..."
    sleep 3
done

echo "All requests completed."