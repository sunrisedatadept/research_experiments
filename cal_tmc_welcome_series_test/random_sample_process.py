# load the necessary packages
import pandas as pd
import requests
from parsons import Redshift, Table, VAN, S3, utilities
from requests.auth import HTTPBasicAuth
import time
from datetime import datetime, timedelta, date
import os
import logging
import json
import time
from urllib.parse import urljoin
import sys
import random
import dictlib
import numpy as np
import base64
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (Mail, Attachment, FileContent, FileName, FileType, Disposition)
# Set seed
random.seed(10)

# Set parameters
delta  = timedelta(hours =  24) ## Set this to the frequency of your Container Script

#If running on container, load this env
try:
    os.environ['REDSHIFT_PORT']
    os.environ['REDSHIFT_DB'] = os.environ['REDSHIFT_DATABASE']
    os.environ['REDSHIFT_HOST']
    os.environ['REDSHIFT_USERNAME'] = os.environ['REDSHIFT_CREDENTIAL_USERNAME']
    os.environ['REDSHIFT_PASSWORD'] = os.environ['REDSHIFT_CREDENTIAL_PASSWORD']
    os.environ['S3_TEMP_BUCKET'] = 'parsons-tmc'
    os.environ['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']
    van_key = os.environ['VAN_PASSWORD']
    send_grid_api_key = os.environ['SEND_GRID_PASSWORD']

#If running locally, load this env
except KeyError:
    os.environ['REDSHIFT_PORT']
    os.environ['REDSHIFT_DB']
    os.environ['REDSHIFT_HOST']
    os.environ['REDSHIFT_USERNAME']
    os.environ['REDSHIFT_PASSWORD']
    os.environ['S3_TEMP_BUCKET']
    os.environ['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']
    van_key = os.environ['VAN_API_KEY']
    send_grid_api_key = os.environ['SENDGRID_API_KEY']

# Set EA API credentials
username = 'welcomeseries'  ## This can be anything
db_mode = '1'    ## Specifying the NGP side of VAN
password = f'{van_key}|{db_mode}' ## Create the password from the key and the mode combined
everyaction_auth = HTTPBasicAuth(username, password)
everyaction_headers = {"headers" : "application/json"}

# Instatiate Redshift instance
rs = Redshift()

# Instantiate VAN instance
van = VAN(api_key=van_key, db='EveryAction')

##### Set up logger #####
logger = logging.getLogger(__name__)
_handler = logging.StreamHandler()
_formatter = logging.Formatter('%(levelname)s %(message)s')
_handler.setFormatter(_formatter)
logger.addHandler(_handler)
logger.setLevel('INFO')


#### Functions
def get_every_action_contacts(everyaction_headers, everyaction_auth):
    """
    Prepares the time strings for the EA API end point, creates the URL end point
    and sends a request to the endpoint for a Contacts record, with VanID, first name,
    last name, phone, SMS opt in status, and the date the contact was created.

    Returns endpoint with the jobId for the download job to access the requested contacts.
    """

    # Prepare vstrings for Changed Entites API
    max_time = datetime.now()
    min_time = max_time - delta
    max_time_string = max_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    min_time_string = min_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    # EveryAction Changed Entities parameters
    base_everyaction_url = 'https://api.securevan.com/v4/'
    everyaction_job = "changedEntityExportJobs"
    changed_entities_url = urljoin(base_everyaction_url, everyaction_job)

    recent_contacts = {
      "dateChangedFrom": 		min_time_string,
      "dateChangedTo" : 		max_time_string,
      "resourceType": 			"Contacts",
      "requestedFields": 		["VanID", "FirstName", "LastName", "Phone", "PhoneOptInStatus", "DateCreated" ],
      "excludeChangesFromSelf": "true"
    }

    response = requests.post(changed_entities_url, json = recent_contacts, headers = everyaction_headers, auth = everyaction_auth, stream = True)
    jobId = str(response.json().get('exportJobId'))
    everyaction_download_url = f'{changed_entities_url}/{jobId}'
    return everyaction_download_url


def get_export_job(everyaction_download_url, everyaction_headers, everyaction_auth):
    """
    Takes the endpoint for the download job and checks if the downlink is available every 20 seconds. Once the download link is available,
    downloads the data into a data frame. If 1000 seconds have passed and the download link is not available, assume the API has stalled out and
    exit the program to try again the next run.
    """

    timeout = 1000   # [seconds]
    timeout_start = time.time()

    while time.time() < timeout_start + timeout:
    	time.sleep(20) # twenty second delay
    	try:
            response = requests.get(everyaction_download_url, headers = everyaction_headers, auth = everyaction_auth)
            logger.info(response)
            downloadLink = response.json().get('files')[0].get('downloadUrl')
            break
    	except:
    		logger.info("File not ready, trying again in 20 seconds")

    if time.time() >= timeout_start + timeout:
    	sys.exit("Export Job failed to download!")
    else:
    	logger.info('Export Job Complete!')
    return downloadLink

def prepare_data(downloadLink):
    """
    Takes the downloaded dataframe of contacts and
    - Checks if contacts were created the day before

    EveryAction returns a date, not a datetime, for DateCreated
    We are relying on Strive's dedupe upsert logic to not text people twice

    Then returns the final data frame that will be send to Strive.
    """

    df = pd.read_csv(downloadLink)
    # Save a csv for troubleshooting
    if len(df) > 0:
        logger.info(f"Found {len(df)} modified contacts. Checking if created today.")
    else:
        sys.exit("No new contacts. Exiting.")

    # Filter for contacts that were created today
    df['DateCreated']= pd.to_datetime(df['DateCreated'], format='%Y-%m-%d')

    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    new_contacts = df.loc[df['DateCreated'] ==  yesterday]

    # Filter for contacts with a phone number
    new_contacts = new_contacts.dropna(subset=['Phone'])

    # Fill in missing names with Friend
    new_contacts['FirstName'] = new_contacts['FirstName'].replace(np.nan, 'Friend')

    if len(new_contacts) > 0:
        logger.info(f"Found {len(new_contacts)} new contacts.")
    else:
        sys.exit("No contacts that were created today. Exiting.")

    # Reduce dataframe to key columns
    new_contacts = new_contacts[["VanID", "FirstName", "LastName", "Phone"]]
   
    return new_contacts

def remove_exclude_from_welcome_series(new_contacts, everyaction_headers):

    # URL to access the Activist Code endpoint
    base_url = "https://api.securevan.com/v4/people/"
    job = "activistCodes"

    # For every row in the new contacts table check if the contact has the "ExFromWelcomeSeries" activist code applied
    # If so, drop the contact
    for index, contact in new_contacts.iterrows():

        # select the vanid
        vanid = contact["VanID"]

        # Connect to the API endpoint 
        activist_code_url = base_url + str(vanid) + "/" + job
        response = requests.get(
            activist_code_url, headers=everyaction_headers, auth=everyaction_auth
        )

        # Drop contacts that have the "ExFromWelcomeSeries"
        activist_codes = json.loads(response.text)["items"]

        if "ExFromWelcomeSeries (Public)" in activist_codes:
           new_contacts = new_contacts.drop(vanid)

    return new_contacts

def randomize_participants(new_contacts):
    """
    Takes the downloaded new contacts and randomly sorts them into one of 3 trial groups.

    Group 1: Tuesday Welcome Call
    Group 2: Wednesday Anytime Action
    """
    list_vanid = new_contacts['VanID'].tolist()
    random.shuffle(list_vanid)
    n = 2 # number of groups
    randomized_participants = np.array_split(list_vanid, 2)

    return randomized_participants

def combineDictList(*args):
    """
    Helper function to combine values of n dictionaries into one dictionary with the same keys
    """
    result = {}
    for dic in args:
        for key in (result.keys() | dic.keys()):
            if key in dic:
                result.setdefault(key, []).extend(dic[key])
    return result


def sort_participants(randomized_participants):
    """
    Takes the randomized participants and appends the group assignment name.
    Reassambles the separate groups into one dictionary/sample
    """

    group_tuesday = {"vanid": randomized_participants[0].tolist(), "participant_group" : ["Tuesday Welcome Call"] * len(randomized_participants[0]) }
    group_wednesday = {"vanid" : randomized_participants[1].tolist(), "participant_group" : ["Wednesday Anytime Action"] * len(randomized_participants[1])}
    sorted_participants = combineDictList(group_tuesday, group_wednesday)

    return sorted_participants

def select_participants(group, sorted_participants, new_contacts):
    """
    Partition the new contacts dataframe into groups
    """
    sorted_participants_df = pd.DataFrame.from_dict(sorted_participants)
    group_vanids = sorted_participants_df.loc[sorted_participants_df['participant_group'] == group]['vanid']
    participants = new_contacts.loc[new_contacts['VanID'].isin(group_vanids)]

    return participants

def send_email(group, csv_name, to_email, subject):
    message = Mail(
        from_email='brittany@sunrisemovement.org',
        to_emails=to_email, #
        subject=subject,
        html_content='Here is your CSV')

    group.to_csv(csv_name)
    with open(csv_name, 'rb') as f:
        data = f.read()
        f.close()
    encoded_file = base64.b64encode(data).decode()

    attachedFile = Attachment(
        FileContent(encoded_file),
        FileName(csv_name),
        FileType('text/csv'),
        Disposition('attachment')
    )
    message.attachment = attachedFile

    sg = SendGridAPIClient(send_grid_api_key)
    response = sg.send(message)
    print(response.status_code, response.body, response.headers)


def push_to_redshift(sorted_participants):
    """
    Take the participant grouping and push to Redshift.
    """
    existing_vanids = rs.query("""
                    select vanid from sunrise.welcome_email_experiment_participants
                    """)

    new_vanids = pd.DataFrame.from_dict(sorted_participants)
    # Reorder dataframe with vanid in uniqueid spot
    new_vanids = new_vanids[['participant_group', 'vanid']]
    # Add created_at column
    new_vanids['tested_at'] = date.today()
    # Remove existing vanids from new_vanids
    new_vanids = new_vanids[~new_vanids['vanid'].isin(existing_vanids)]
    # Convert dataframe to Parsons table for copy to Redshift
    result_table = Table.from_dataframe(new_vanids)

    # copy Table into Redshift, append new rows
    rs.copy(result_table, 'sunrise.welcome_email_experiment_participants' ,if_exists='append', distkey='vanid', sortkey = None, alter_table = True)

def apply_activist_code(table, activist_code):
    response = [{"activistCodeId": activist_code,
             "action": "Apply",
             "type": "ActivistCode"}
            ]

    for index, row in table.iterrows():
        van.apply_response(row['vanid'], response)

if __name__ == "__main__":
    logger.info("Initiate Export Job")
    everyaction_download_url = get_every_action_contacts(everyaction_headers, everyaction_auth)
    downloadLink = get_export_job(everyaction_download_url, everyaction_headers, everyaction_auth)
    new_contacts = prepare_data(downloadLink)
    new_contacts = remove_exclude_from_welcome_series(new_contacts, everyaction_headers)
    logger.info("Randomize participants")
    randomized_participants = randomize_participants(new_contacts)
    sorted_participants = sort_participants(randomized_participants)
    # Send group of new participants to Redshift 
    push_to_redshift(sorted_participants)
    
    # Separate sorted participants into three groups and apply correct columns
    tuesday_participants = select_participants("Tuesday Welcome Call", sorted_participants, new_contacts)
    tuesday_participants.columns = ["vanid", "firstName", "lastName", "cell"]
    
    wednesday_participants = select_participants("Wednesday Anytime Action", sorted_participants, new_contacts)
    wednesday_participants.columns = ["vanid", "firstName", "lastName", "cell"]
    
    apply_activist_code(tuesday_participants, 4965345)
    apply_activist_code(wednesday_participants, 4965347)

    # Send 3 separate emails to texting team for each group    
    send_email(tuesday_participants, "tuesday_welcome_call_participants.csv", ["tnt@nagog.com", "jasy@sunrisemovement.org"], 'Tuesday Welcome Call Participants')
    send_email(wednesday_participants, "wednesday_anytime_action_call_participants.csv", ["tnt@nagog.com", "jasy@sunrisemovement.org"], 'Wednesday Anytime Action Participants')
