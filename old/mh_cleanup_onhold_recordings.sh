#!/bin/bash

ADMIN_URL="http://localhost:8080"
DIGEST_USER="matterhorn_system_account"
DIGEST_PASS="CHANGE_ME"

STORAGE_ROOT="/opt/matterhorn/storage"
TRASH_ROOT="$STORAGE_ROOT/.trash"

FILES_DIR="$STORAGE_ROOT/files/mediapackage"
WS_DIR="$STORAGE_ROOT/workspace/mediapackage"


USAGE="Usage: $0 [-f] [-u] [-n] <workflow-ID>
\t-f : Force deletion of mediapackage.
\t-u : Unpublish the media associated with this workflow.
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
	-u)
	    unpublish=true
	    ;;
	-*)
	    echo "Unknow flag: $1" >&2
	    echo -e "$USAGE" >&2
	    exit 1
	    ;;
    esac
    shift
done


if [ "$1" ]; then 

    echo -n "Stopping the workflow $1... "
    result=$(curl -s --digest -u "$DIGEST_USER":"$DIGEST_PASS" -H 'X-Requested-Auth: Digest' -F id=$1 $ADMIN_URL/workflow/stop) 

    state=$(echo "$result" | grep -m1 -o '<\([^:>]\+:\)\?workflow[^>]*>' | grep -o 'state="[^"]\+"' | cut -d\" -f2 )

    if [[ "$state" != "STOPPED" ]]; then
	echo $state
        echo "Error! The endpoint did not return a valid workflow"
        exit 1
    else
        echo "Done!"
    fi

    mp_id=$(echo "$result" | grep -m1 -o '<\([^:>]\+:\)\?mediapackage[^>]*>' | grep -o 'id="[^"]\+"' | cut -d\" -f2 )
    
    files="$FILES_DIR/$mp_id"
    ws="$WS_DIR/$mp_id"
    
    # Unpublish?
    if [ "$unpublish" ]; then
       [ "$force" ] && force_txt="-f"
       [ "$no_backup" ] && nb_txt="-n"
       
       ./unpublish.sh $force_txt $nb_txt "$mp_id"
    fi


    echo -e "\nRemoving mediapackage contents..."

    # Try to move the contents to the trash
    no_directories=true
    for path in "${FILES_DIR%/}" "${WS_DIR%/}"; do
	if [ -d "$path/$mp_id" ]; then 
	    unset no_directories
	    trashpath="${TRASH_ROOT%/}/${path#${STORAGE_ROOT%/}/}"
	    if [ ! "$force" ]; then
		echo -en "\nDo you wish to move\n\t$path/$mp_id\nto trash? (Y|N): " 
		read -e answer
		while [ "$answer" != "Y" -a "$answer" != "N" ]; do
		    read -ep "Please write 'Y' or 'N': " answer
		done
	    fi
	    if [ "$force" -o "$answer" = "Y" ]; then
		if [ "$no_backup" ]; then
		    echo -n "Removing $path/$mp_id from disk... "
		    rm -rf "$path/$mp_id"
		else
		    echo -n "Moving $path/$mp_id to trash... "
		    mkdir -p "${trashpath}"
		    destination="$trashpath/$mp_id"
		    i=0
		    while [ -e "$destination" ]; do
			destination="${destination%.~$i~}.~$((++i))~"
		    done
		    mv "$path/$mp_id" "$destination"
		fi
		echo "Done!"
	    else
		echo "Directory NOT moved to trash"
	    fi
	fi
    done
    
    if [ "$no_directories" ]; then
        echo "There are no directories for this MediaPackage ID, so nothing to delete!!!"
    fi

else
    echo -e "$USAGE" >&2
    exit 1
fi
