#!/bin/bash
# verify file integrity by cmp command
# This script verifies the integrity of the transferred files by comparing them with the original files.
# verify all file with the same name that have parttern *B.zip

for file in *B.zip; do
    # check if the file exists
    if [ -f "$file" ]; then
        # get the name of the file without the extension
        filename="${file%.*}"
        # compare the file with the original file
        if cmp -s "$file" "base/$filename.zip"; then
            echo "$file is identical to $filename.zip"
        else
            echo "$file is different from $filename.zip"
        fi
    else
        echo "$file does not exist"
    fi
done