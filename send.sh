#!/bin/bash
BASE_URL="http://localhost:8000"

echo "Start"
for i in {1..10}; do
    MESSAGE="id$i"
    RESPONSE=$(curl -s -X 'POST' "$BASE_URL/post_message" \
        -H 'Content-Type: application/json' \
        -d "{\"msg\": \"$MESSAGE\"}")
    echo "Message $i response: $RESPONSE"
done
