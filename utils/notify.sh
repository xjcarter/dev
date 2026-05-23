#!/bin/bash
#notify.sh - wrapper of python strategies that sends email notifications
# of cronjob starts, error, and finishes
# it also handles 'hub' use for applications the connect to IBRK via hub proxy
# Usage: notify.sh "<hub=true>" "strategy1.py --param1=this, --param2=that ..."

#set -

source /trading/utils/env.sh

# 1. Display help if no arguments are passed
if [ "$#" -eq 0 ]; then
    echo "Usage: $0 [hub] <cmd>"
    echo "  Examples:"
    echo "    $0 my_command          -> hub: (), cmd: (my_command)"
    echo "    $0 hub=true my_command -> hub: (hub=true), cmd: (my_command)"
    exit 0
fi

hub=''
cmd_script=''

# 2. Logic for 1 vs 2 arguments
case $# in
    1)
        cmd_script=$1
        hub=''
        ;;
    2)
        hub=$1
        cmd_script=$2
        ;;
    *)
        echo "Error: Too many arguments (max 2)."
        exit 1
        ;;
esac

# 3. Logic for exporting USE_HUB based on the "hub" string
if [ "$hub" == "hub=false" ]; then
    export USE_HUB='FALSE'
elif [ "$hub" == "hub=true" ]; then
    export USE_HUB='TRUE'
fi

# get the hostname of the machine
machine=$(hostname)


# Define an array of recipient email addresses
#recipients=("xjcarter@gmail.com" "jcarter@hannibalinvestments.com")
recipients=("xjcarter@gmail.com")

# Initialize the email body
email_body=""

# Function to add messages to the email body
add_message() {
  now=$(date +"%Y.%m.%d %H.%M.%S: ")
  email_body="$email_body$now $1\n"
}

# Step 1: Notify the user that the Python script is starting via email
add_message "Starting- '$cmd_script'"

# Loop through the recipients and send the "Starting..." email
for recipient in "${recipients[@]}"; do
  subject="NOTIFY START [$machine]: $cmd_script"
  python3 -c "import email_lib; email_lib.send_email(\"$recipient\", \"$subject\", \"$email_body\")"
done

# Run the provided Python script and capture the output
output=$( $cmd_script 2>&1)

# Capture the exit code of the Python script
exit_code=$?

# Step 2: Check the exit code and notify the user accordingly
if [ $exit_code -eq 0 ]; then
  add_message "$cmd_script completed successfully."
  subject="NOTIFY SUCCESS [$machine]: $cmd_script"
  for recipient in "${recipients[@]}"; do
    python3 -c "import email_lib; email_lib.send_email(\"$recipient\", \"$subject\", \"$email_body\")"
  done
else
  add_message "Failure: $cmd_script .  Exit code= $exit_code."
  subject="NOTIFY FAILURE [$machine]: $cmd_script"

   # Save output to a temporary file
  tmp_file=$(mktemp)
  echo -e "$output" > "$tmp_file"
  for recipient in "${recipients[@]}"; do
    python3 -c "import email_lib; email_lib.send_email(\"$recipient\", \"$subject\", \"$email_body\", \"$tmp_file\", \"error_log.txt\")"
  done 
  rm $tmp_file
fi


