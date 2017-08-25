#! /bin/bash

DEFAULT_DB_URL=hordak.rrz.uni-koeln.de
DEFAULT_DB_NAME=matterhorn_test
DEFAULT_DB_USER=matterhorn
DEFAULT_DB_PASS=2mdmU4xTmW3RqqFD
DEFAULT_ORGANIZATION="mh_default_org"


############### NO CHANGES BELOW THIS POINT #####################


USAGE="Usage: $0 [-f] [-d DB_NAME] [-s DB_URL] [-u DB_USER] [-q DB_PASS] [-o ORGANIZATION] [-p PASSWORD] [--] [USERNAME [ROLE1 [ROLE2 ...]]]

Inserts a new user in the Matterhorn database.

All parameters are optional. If omitted, defaults (specified at the beginning of the script file) are applied. 
If no default is defined, the parameter is requested interactively.

\tUSERNAME       : Name of the new Matterhorn user.
\tPASSWORD       : Password of the new user.
\tROLE#          : List of roles to assign to the new user.
\tORGANIZATION   : The organization this user belongs to in Matterhorn.
\tDB_NAME        : Name of the Matterhorn database.
\tDB_URL         : Location (URL) of the Matterhorn database server.
\tDB_USER        : Name of the Matterhorn database user.
\tDB_PASS        : Password of the Matterhorn database user.

\t-f             : Do not ask for confirmation.
\t--             : Separate options from the rest of the arguments.
"


# $1 the variable name
# $2 the default value
check_param() {
    # Check whether the parameter is defined, or assign it to its default value
    eval $1=${!1:=$2}

    if [ -z "${!1}" ]; then
        # Request the parameter, if still empty
	while [ -z "${!1}" ]; do
	    read -ep "Enter the new $1: " $1
	    
            # Exit the loop if username not empty
	    [ "${!1}" ] && break
	    
	    echo "Error: The parameter $1 must not be empty." 2>&1
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
	    
		echo "Error: The password for $2 must not be empty." 2>&1
		echo
	    done
	
            # Password check
	    IFS='' read -sep "Repeat the password: " pass_check
	    echo
	    if [ "${!1}" = "$pass_check" ]; then
		break
	    fi
	    
	    unset "$1"
	    echo "Error: Passwords do not match." 2>&1
	    echo
	done   
	echo
    fi
}


###################### MAIN ########################

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
	    echo "Unknow flag: $1" >&2
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
for param in db_url db_name db_user; do
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

# Make sure the organization is already defined, or request it
check_param organization "$DEFAULT_ORGANIZATION"

# Request this user's password
check_password password "$username"

# Ask for the roles
check_param roles

roles=( ${roles//,/ } )

# Prints a summary of the parameters to confirm
cat -T <<EOF 
Details of the operation:
========================
USERNAME          : $username
ROLES             : ${roles[@]}
ORGANIZATION      : $organization
DATABASE URL      : $db_url
DATABASE NAME     : $db_name
DATABASE USER     : $db_user
EOF

echo

if [ -z "$force" ]; then
    read -n1 -e -p "Do you wish to proceed? [y|N]: " proceed
    echo
fi


if [[ "$proceed" = [yY] || "$force" ]]; then
    # Generate query to insert all roles
    unset role_query
    if [ "${#roles[@]}" -gt 0 ]; then
	for role in ${roles[@]}; do
	    role_query="$role_query, ('$username', '$organization', '$role')"
	done
	role_query="INSERT INTO mh_role (username, organization, role) VALUES${role_query#,};"
    fi
    
    mysql -h "$db_url" -u "$db_user" -p"$db_pass" "$db_name" <<EOF
# Insert the name into the database
INSERT INTO mh_user (username, organization, password) VALUES ('$username', '$organization', MD5('$password{$username}')); 

# If some roles have been specified, the following line inserts them. Otherwise it's empty
$role_query
EOF
    
[ $? -eq 0 ] && echo "Done!" || echo "Error: Operation failed." 2>&1

else
    echo "Operation cancelled by the user"
fi
