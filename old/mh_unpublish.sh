#!/bin/bash

# Maximum number of times to poll the engage server to see whether
# the jobs are finished
MAX_ATTEMPTS=10

# Waiting time between two polls to the engage server
TIME_WAIT=5 #seconds

ADMIN_URL="http://localhost:8080"
ENGAGE_URL="$ADMIN_URL"

DIGEST_USER="matterhorn_system_account"
DIGEST_PASS="CHANGE_ME"


STORAGE_ROOT="/opt/matterhorn/storage"
TRASH_ROOT="$STORAGE_ROOT/.trash"

DOWNLOADS_DIR="$STORAGE_ROOT/downloads"
STREAMING_DIR="$STORAGE_ROOT/streaming"

# As per Matterhorn 1.4.4, this is the only dist. channel defined
DISTRIBUTION_CHANNELS="engage-player"


USAGE="Usage: $0 [-f] [-n] <mediapackage-ID>
\t-f : Force deletion of mediapackage.
\t-n : Don't back up files. Simply delete them."


while [[ "$1" =~ ^- ]]; do
    case "$1" in
	--)
	    shift
	    break
	    ;;
	-f)
	    force=true
	    ;;
	-n)
	    no_backup=true
	    ;;
	-*)
	    echo "Unknow flag: $1" >&2
	    echo "$USAGE" >&2
	    exit 1
	    ;;
    esac
    shift
done


if [ "$1" == '' ]; then
    echo -e "$USAGE" >&2
    exit 1
fi

# Make sure the ENGAGE_URL does not end in a '/'
ENGAGE_URL=${ENGAGE_URL%/}

tmp_file="/tmp/mediap"

# Get distributed mediapackage
echo -n "Obtaining mediaPackage from Search..... "
# Requests the Mediapackage from search and filters the answer using grep. The "exit ${PIPESTATUS[0]}" part is for detecting errors in curl

raw="$(curl -s --digest -u "$DIGEST_USER":"$DIGEST_PASS" -H 'X-Requested-Auth: Digest' -H 'X-Opencast-Matterhorn-Authorization: true' "${ENGAGE_URL%/}/search/episode.xml?id=$1" 2> /dev/null; \
exit ${PIPESTATUS[0]} )"

# Detect if curl is not installed
if [ "$?" -eq 127 ]; then
    echo "Error! 'curl' not found."
    echo "This script depends on the 'curl' command to communicate with Matterhorn. Please install 'curl' and try again."
    exit 1
fi

namespaces=( $(echo $raw | grep -o 'xmlns\(:[^=]\+\)\?="[^"]\+"') )
namespaces=${namespaces[@]//\"/\\\"}

mp="$(echo $raw | grep -o -m 1 '<\([^:>/]\+:\)\?mediapackage[^>]*>.*</\1mediapackage>' | sed "s#<\([^:>/]\+:\)\?mediapackage\([^>]*\)>#<\1mediapackage\2 $namespaces>#" )"

if [ "$mp" ]; then
    echo "Done"
    
    echo -n "Deleting distributed media..... "

    echo "${mp}" > "$tmp_file"

    lists=(media metadata attachments)
    items=(track catalog attachment)
    services=(download streaming)

    unset jobid
    for ((i = 0; i < 3; i++)); do
        # For each mediapackage element types
	list=${lists[$i]}
	item=${items[$i]}
        # Get IDs
	ids=$(echo $mp | grep -o "<\([^:>/]\+:\)\?$list>.*</\1$list>" | grep -o "<\([^:>/]\+:\)\?$item[^>]*>" | grep -o 'id="[^"]\+"' | cut -d\" -f2)

        # Retract each ID from each service in the list (download, streaming)
	for id in ${ids[@]}; do
            for service in ${services[@]}; do
		for dist_channel in $DISTRIBUTION_CHANNELS; do
                    # Create retract job
		    jobid["${#jobid[@]}"]=$(curl -s --digest -i -u "$DIGEST_USER:$DIGEST_PASS" -H 'X-Requested-Auth: Digest' \
			-F mediapackage=@$tmp_file -F elementId="$id" -F channelId="$dist_channel" \
			$ENGAGE_URL/distribution/$service/retract | \
			grep -o '<\([^:>]\+:\)\?job[^>]*>' | \
			grep -o 'id="[^"]\+"' | cut -d\" -f 2)
		done
	    done
	done
    done

    echo -en "Done!\n\nWaiting for the jobs to finish..... "

    unset job_status
    for (( i = 0; i < $MAX_ATTEMPTS; i++ )); do
	# Take some time between iterations
	sleep $TIME_WAIT
	
	all_set=true
	
	for (( j = 0; j < ${#jobid[@]}; j++ )); do
	    if [ "${job_status[$j]}" != 'FINISHED' -a "${job_status[$j]}" != 'FAILED' ]; then
		job_status[$j]=$(curl -s --digest -u "$DIGEST_USER:$DIGEST_PASS" -H 'X-Requested-Auth: Digest' "$ADMIN_URL"/services/job/"${jobid[$j]}".xml |\
                                 grep -m1 -o '<\([^:>]\+:\)\?job[^>]*>' |\
                                 grep -o 'status="[^"]\+"' |\
                                 cut -d\" -f 2)

		if [ "${job_status[$j]}" != 'FINISHED' -a "${job_status[$j]}" != 'FAILED' ]; then
		    unset all_set
		fi
		sleep 1
	    fi
	done

	# Break if all the jobs are done
	[ "$all_set" ] && break
    done

    echo "Done"

    for (( i = 0; i < ${#jobid[@]}; i++ )); do
	echo "Retract job ${jobid[$i]} ended with result: ${job_status[$i]:-}"
    done
    
else
    # The package is not published in the search index.
    echo -e "Not found!\nThe package is not available at the search index."
fi

# Move whatever files there are in downloads and streaming to the trash
# This is done in case the 'retract' operation fails
for dist_type in "${DOWNLOADS_DIR%/}" "${STREAMING_DIR%/}"; do
    for dist_channel in $DISTRIBUTION_CHANNELS; do
	path="$dist_type/$dist_channel/$1"
	if [ -d "$path" ]; then 
	    trashpath="${TRASH_ROOT%/}/${path#${STORAGE_ROOT%/}/}"
	    if [ ! "$force" ]; then
		echo -en "Do you wish to move\n\t$path\nto trash? (Y|N): " 
		read -e answer
		while [ "$answer" != "Y" -a "$answer" != "N" ]; do
		    read -ep "Please write 'Y' or 'N': " answer
		done
	    fi
	    if [ "$force" -o "$answer" = "Y" ]; then
		if [ "$no_backup" ]; then
		    echo -n "Removing $path from disk... "
		    rm -rf "$path"
		else
		    echo -n "Moving $path to trash... "
		    mkdir -p "${trashpath%/$1}"
		    i=0
		    while [ -e "$trashpath" ]; do
			trashpath="${trashpath%.~$i~}.~$((++i))~"
		    done
		    mv "$path" "$trashpath"
		fi
		echo "Done!"
	    else
		echo "Directory NOT moved to trash"
	    fi
	fi
    done
done


echo -n "Removing mediaPackage from Search index..... "

# Get the retracting job
job_id=$(curl -s --digest -u $DIGEST_USER:$DIGEST_PASS -H 'X-Requested-Auth: Digest' -X DELETE $ENGAGE_URL/search/$1 | \
    grep -o '<\([^:>]\+:\)\?job[^>]*>' | \
    grep -o 'id="[^"]\+"' | cut -d\" -f 2)

# Wait for the job to complete
unset job_status
for ((i = 0; i < 10; i++)); do 
    sleep 5
    job_status=$(curl -s --digest -u "$DIGEST_USER:$DIGEST_PASS" -H 'X-Requested-Auth: Digest' "$ADMIN_URL"/services/job/"$job_id".xml |\
        grep -m1 -o '<\([^:>]\+:\)\?job[^>]*>' |\
        grep -o 'status="[^"]\+"' |\
        cut -d\" -f 2)

    if [ "$job_status" == 'FINISHED' ]; then
	echo "Done!"
	exit 0
    elif [ "$job_status" == 'FAILED' ]; then
        echo -e "Error!\nCouldn't delete the MediaPackage from search."
	exit 1
    fi
done

echo -e "Error!\nThe deleting job has not completed yet. It may or may not effectively delete the element from the search index"
exit 2
