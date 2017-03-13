#!/bin/bash

# Script to migrate mediapackages from one Matterhorn instance to other
# Assumes that:
# * In SRC_DIR we have subdirectories named after each Mediapackage ID.
#   Each subdirectory contains, at least, a .zip file with the compressed Mediapackage,
#   including its manifest, as specified by Matterhorn ingest services.
# * In IDS_FILE there is a list of Mediapackages, ideally one per line.
# * Every time this script runs, it loops though SRC_DIR until it finds a folder that
#   contains a .zip file and DOES NOT contain a file named ".ingested". If the 
#   directory name (i.e. the mediapackage ID) is in the file, then the script copies
#   the .zip file into DEST_DIR_IF_IN_FILE. Otherwise, it copies it into 
#   DEST_DIR_IF_NOT_IN_FILE.
#   This IDS_FILE method has been used to apply to different workflows, depending on 
#   whether a mediapackage was already archived or not. 
#
# * NOTE THAT THIS SCRIPT COPIES ONLY ONE FILE PER EXECUTION. The idea is using it
#   in combination with a cron job, so that an eventual error won't stop the migrate
#   process.



HELP="Usage: $0 [SRC_DIR] [IDS_FILE] [DEST_DIR_IF_IN_FILE] [DEST_DIR_IF_NOT_IN_FILE]"
INGESTED=".ingested"


if [ "$#" -ne 4 ]; then
    echo "Incorrect number of parameters" >&2
    echo $HELP >&2
    exit 1
fi

for d in ( "$1" "$3" "$4" ); do
    if [ ! -d "$d" ]; then
	echo "$d is not a directory" >&2
	exit 1
    fi
done

if [ ! -f "$2" ]; then
    echo "$2 is not a valid file" >&2
    exit 1
fi

src_dir="${1%/}"
id_file="$2"
dest_if_file="${3%/}"
dest_not_file="${4%/}"
    

for d in $(ls "$src_dir"); do

    if [ "$(ls "$dest_if_file" | wc -l)" -gt 0 -a "$(ls "$dest_not_file" | wc -l)" -gt 0 ]; then
	echo "No MP copied because all inboxes are busy"
	break
    fi
     
    # Skip non-directories
    [ -d "$src_dir/$d" ] || continue

    # Skip ingested files
    [ -f "$src_dir/${d%/}/$INGESTED" ] && continue
    
    if grep "$d" "$id_file" > /dev/null; then
	dest_dir="$dest_if_file"
    else
	dest_dir="$dest_not_file"
    fi

    if [ "$(ls "$dest_dir" | wc -l)" -eq 0 ]; then
	if cp "$src_dir/${d%/}/${d%/}.zip" "$dest_dir"; then 
	    touch "$src_dir/${d%/}/$INGESTED"
	    break
	else
	    echo "Error copying '$src_dir/${d%/}/${d%/}.zip'" >&2
	fi
    fi
done
