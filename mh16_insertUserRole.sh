#! /bin/bash

DEFAULT_DB_URL=
DEFAULT_DB_NAME=
DEFAULT_DB_USER=
DEFAULT_DB_PASS=
DEFAULT_ORGANIZATION=

# The name of the mysql command
MYSQL=mysql

############### NO CHANGES BELOW THIS POINT #####################


USAGE="
Usage: $0 [-h] [-f] [-d DB_NAME] [-s DB_URL] [-u DB_USER] [-q DB_PASS] [-o ORGANIZATION] [-p PASSWORD] [--] [USERNAME [ROLE1[,DESC1] [ROLE2[,DESC2] ...]]]

Inserts a new user in the Matterhorn database.

All parameters are optional. If omitted, defaults (specified at the beginning of the script file) are applied. 
If no default is defined, the parameter is requested interactively.

\tUSERNAME       : Name of the new Matterhorn user.
\tPASSWORD       : Password of the new user.
\tROLE#          : List of roles to assign to the new user.
\tDESC#          : Description of the role. It will only apply if the role needs to be created.
\tORGANIZATION   : The organization this user belongs to in Matterhorn.
\tDB_NAME        : Name of the Matterhorn database.
\tDB_URL         : Location (URL) of the Matterhorn database server.
\tDB_USER        : Name of the Matterhorn database user.
\tDB_PASS        : Password of the Matterhorn database user.

\t-f             : Do not ask for confirmation.
\t-h             : Show this help message
\t--             : Separate options from the rest of the arguments.
"


# $1 the variable name
# $2 the default value
check_param() {
    # Check whether the parameter is defined, or assign it to its default value
    eval "$1=\"${!1:=$2}\""

    if [ -z "${!1}" ]; then
        # Request the parameter, if still empty
	while [ -z "${!1}" ]; do
	    read -ep "Enter the new $1: " $1
	    
            # Exit the loop if username not empty
	    [ "${!1}" ] && break
	    
	    echo "Error: The parameter $1 must not be empty." >&2
	    echo
	done
	echo
    fi
}


# $1 the variable to assign the password to
# $2 the username this password corresponds to
# $3 the password's default value (if any)
check_password() {

    local pass_check

    # Check whether the password is defined, or assign it to its default value
    eval $1=${!1:=$3}

    # Ask for the password and check it's correctly spelled
    if [ -z "${!1}" ]; then
	while [ -z "${!1}" ]; do
	
            # Make sure the password isn't empty
	    while [ -z "${!1}" ]; do
		IFS='' read -sep "Enter the password for $2: " $1
		echo
	    
    	        # Exit the loop if password not empty
		[ "${!1}" ] && break
	    
		echo "Error: The password for $2 must not be empty." >&2
		echo
	    done
	
            # Password check
	    IFS='' read -sep "Repeat the password: " pass_check
	    echo
	    if [ "${!1}" = "$pass_check" ]; then
		break
	    fi
	    
	    unset "$1"
	    echo "Error: Passwords do not match." >&2
	    echo
	done   
	echo
    fi
}


###################### MAIN ########################

# Make sure the mysql command is installed
if ! hash $MYSQL 2> /dev/null; then
    echo "Error: The required command '$MYSQL' cannot be found. Please make sure it is installed and in your \$PATH" >&2
    exit 1
fi


# Process command line arguments
while [[ "$1" =~ ^- ]]; do
    case "$1" in
	--)
	    shift
	    break
	    ;;
	-d)
	    db_name="$2"
	    shift
	    ;;

	-f)
	    force=true
	    ;;
	-h)
	    echo -e "$USAGE" >&2
	    exit 0
	    ;;
	-o)
	    organization="$2"
	    shift
	    ;;
	-p) 
	    password="$2"
	    shift
	    ;;
	-q) 
	    db_pass="$2"
	    shift
	    ;;
	-s) 
	    db_url="$2"
	    shift
	    ;;
	-u)
	    db_user="$2"
	    shift
	    ;;
	-*)
	    echo "Unknown flag: $1" >&2
	    echo -e "$USAGE" >&2
	    exit 1
	    ;;
    esac
    shift
done

# Assign the rest of the command line
username="$1"
shift
roles="$@"

# Check the parameters for the database
for param in db_url db_name db_user organization; do
    # Get the default param's name corresponding to this one
    # For instance, for db_url you've got DEFAULT_DB_URL
    default_param="DEFAULT_$(echo $param | tr [:lower:] [:upper:])"
    
    # Make sure the parameter get assigned
    check_param "$param" "${!default_param}"
done

# Make sure the db_pass is set
check_password db_pass "$db_user" "$DEFAULT_DB_PASS"

# Make sure the new user name is defined, or request it
check_param username

# Request this user's password
check_password password "$username"

# Ask for the roles
check_param roles

# Split roles and descriptions
roles=( ${roles} )
for (( i = 0; i < ${#roles[@]}; i++ )); do
    if [[ "${roles[$i]}" =~ , ]]; then
	# Separate what is before and after the first comma
	role_descriptions[$i]=${roles[$i]#*,}
	roles[$i]=${roles[$i]%%,*}
	if [ -z "$roles[$i]" ]; then
	    echo "Error. Role names can not be empty" >&2
	    exit 1
	fi
    fi
done

# Check those roles exist
max_len=0
for (( i = 0; i < ${#roles[@]}; i++ )); do
    role=${roles[$i]}

    # Get the role length
    if [ ${#role} -gt $max_len ]; then
	max_len=${#role}
    fi

    # Check if the role already exists
    role_id=$($MYSQL -h "$db_url" -u "$db_user" -p"$db_pass" "$db_name" -Bs -e "select id from mh_role where name='${role}' and organization='${organization}'")

    if [ -z "$role_id" ]; then
	echo "Role '${role}' does not exist in the organization '${organization}', so it will be created."
	
	# Ask for a description, only if it is not defined or empty
	if [ -z ${role_descriptions[$i]+x} ]; then
	    unset want_desc role_description
	    read -n1 -p "Do you wish to add description to this role? [y|N]: " want_desc
	    [ -n "$want_desc" ] && echo
	    while [[ ! "$want_desc" =~ ^[yYnN]?$ ]]; do
		read -n1 -p "Please enter (y)es or (n)o: " want_desc
		[ -n "$want_desc" ] && echo
	    done
	    if [[ "$want_desc" =~ ^[yY]$ ]]; then
		desc_ok='n'
		while [[ ! "$desc_ok" =~ ^[yY]?$ ]]; do
		    read -ep "Please enter the role description: " role_description
		    echo
		    echo "\"$role_description\""
		    read -n1 -p "Is this OK? [Y|n]: " desc_ok
		    [ -n "$desc_ok" ] && echo
		    while [[ ! "$desc_ok" =~ ^[yYnN]?$ ]]; do
			read -n1 -p "Please enter (y)es or (n)o: " desc_ok
			[ -n "$desc_ok" ] && echo
		    done
		done
	    fi
	    echo
	    role_descriptions[$i]="$role_description"
	fi
    else
	role_ids[$i]="$role_id"
	role_descriptions[$i]=$($MYSQL -h "$db_url" -u "$db_user" -p"$db_pass" "$db_name" -Bs -e "select description from mh_role where id=${role_id}")
    fi
done

cat -T <<EOF 

Details of the operation:
========================
USERNAME          : $username
ORGANIZATION      : $organization
DATABASE URL      : $db_url
DATABASE NAME     : $db_name
DATABASE USER     : $db_user
EOF

# Prints a summary of the parameters to confirm
if [ ${#roles[@]} -gt 0 ]; then
    echo
    echo "ROLES:"
    for (( i = 0; i < ${#roles[@]}; i++ )); do
	[ -z "${role_ids[$i]}" ] && echo -n "(new) " || echo -n "      "
	echo "${roles[$i]}$(seq -s' ' $(( $max_len - ${#roles[$i]} + 2 )) | tr -d '[:digit:]'): \"${role_descriptions[$i]:-<NO DESCRIPTION>}\""
    done
fi


echo

if [ -z "$force" ]; then
    read -n1 -e -p "Do you wish to proceed? [y|N]: " proceed
    echo
    while [[ ! "$proceed" =~ ^[yYnN]?$ ]]; do
	read -n1 -e -p "Please enter (y)es or (n)o: " proceed
	echo
    done
fi

if [[ "$proceed" = [yY] || "$force" ]]; then

    # Get the maximum role index (the autoincrement does not work for some reason
    max_user_id=$($MYSQL -h "$db_url" -u "$db_user" -p"$db_pass" "$db_name" -Bs -e "select max(id) from mh_user;")
    if [ -z "$max_user_id" -o "$max_user_id" == "NULL" ]; then
	max_user_id=-1
    fi
   
    # Create the user
    $MYSQL -h "$db_url" -u "$db_user" -p"$db_pass" "$db_name" \
	-e "INSERT INTO mh_user (id, username, organization, password) VALUES ($(( ++max_user_id)), '$username', '$organization', MD5('$password{$username}'));"
    if [ $? -ne 0 ]; then
	echo "Error. Couldn't create user!" >&2
	exit 1
    fi

    # Get user id
    user_id=$($MYSQL -h "$db_url" -u "$db_user" -p"$db_pass" "$db_name" -Bs -e "select id from mh_user where username='$username' and organization='$organization';")

    # Generate query to insert all roles
    unset role_query
    if [ "${#roles[@]}" -gt 0 ]; then
	# Get the maximum role index (the autoincrement does not work for some reason
	max_role_id=$($MYSQL -h "$db_url" -u "$db_user" -p"$db_pass" "$db_name" -Bs -e "select max(id) from mh_role;")
	if [ -z "$max_role_id" -o "$max_role_id" == "NULL" ]; then
	    max_role_id=-1
	fi

	for (( i = 0; i < ${#roles[@]}; i++ )); do
	    if [ -z "${role_ids[$i]}" ]; then
		# Create the new role
		$MYSQL -h "$db_url" -u "$db_user" -p"$db_pass" "$db_name" \
		    -e "insert into mh_role (id, organization, name, description) values ($(( ++max_role_id )), '$organization', '${roles[$i]}', '${role_descriptions[$i]}');"

		# Get the role's ID
		role_ids[$i]=$($MYSQL -h "$db_url" -u "$db_user" -p"$db_pass" "$db_name" -Bs -e "select id from mh_role where name='${roles[$i]}' and organization='${organization}'")
		if [ $? -ne 0 ]; then
		    echo "Error. Couldn't create role ${roles[$i]}!" >&2
		    exit 1
		fi
	    fi
	    role_query="$role_query, ('$user_id', '${role_ids[$i]}')"
	done

	# Put roles in the database
	$MYSQL -h "$db_url" -u "$db_user" -p"$db_pass" "$db_name" -e "INSERT INTO mh_user_role (user_id, role_id) VALUES${role_query#,};"
    fi
    
[ $? -eq 0 ] && echo "Done!" || echo "Error: Operation failed." >&2

else
    echo "Operation cancelled by the user"
fi
