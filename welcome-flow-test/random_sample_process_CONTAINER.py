# load the necessary packages
import pandas as pd
import requests
from parsons import Redshift, Table, VAN, S3, utilities
from requests.auth import HTTPBasicAuth
import time
from datetime import datetime, timedelta
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

# Civis environmental variables
os.environ['REDSHIFT_PORT']
os.environ['REDSHIFT_DB'] = os.environ['REDSHIFT_DATABASE']
os.environ['REDSHIFT_HOST']
os.environ['REDSHIFT_USERNAME'] = os.environ['REDSHIFT_CREDENTIAL_USERNAME']
os.environ['REDSHIFT_PASSWORD'] = os.environ['REDSHIFT_CREDENTIAL_PASSWORD']
os.environ['S3_TEMP_BUCKET'] = 'parsons-tmc'
os.environ['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']
van_key = os.environ['VAN_PASSWORD']
strive_key = os.environ['STRIVE_PASSWORD']
campaign_id = os.environ['STRIVE_CAMPAIGN_ID']
send_grid_api_key = os.environ['SEND_GRID_PASSWORD']

# Set EA API credentials
username = 'welcometext'  ## This can be anything
db_mode = '1'    ## Specifying the NGP side of VAN
password = f'{van_key}|{db_mode}' ## Create the password from the key and the mode combined
everyaction_auth = HTTPBasicAuth(username, password)
everyaction_headers = {"headers" : "application/json"}

# Initiate Redshift instance
rs = Redshift()

# Strive parameters
strive_url = "https://api.strivedigital.org/"

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
    		downloadLink = response.json().get('files')[0].get('downloadUrl')
    		break
    	except:
    		logger.info("File not ready, trying again in 20 seconds")

    if time.time() == timeout_start + timeout:
    	sys.exit("Export Job failed to download!")
    else:
    	logger.info('Export Job Complete!')
    return downloadLink

def prepare_data(downloadLink):
    """
    Takes the downloaded dataframe of contacts and
    - Checks if contacts were created today
    - Checks if contacts are opted in to SMS list

    EveryAction returns a date, not a datetime, for DateCreated
    # Relying on Strive's dedupe upsert logic to not text people twice

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

    if len(new_contacts) > 0:
        logger.info(f"Found {len(new_contacts)} new contacts. Checking if they are opted in.")
    else:
        sys.exit("No contacts that were created today. Exiting.")

    # Filter for contacts that have opted in. Opted in = 1
    new_contacts = new_contacts.loc[new_contacts['PhoneOptInStatus'] == 1.0]
    new_contacts = new_contacts[["VanID", "FirstName", "LastName", "Phone"]]
    if len(new_contacts) != 0:
        logger.info("New folk to welcome! Let's send to Strive. They'll handle any deduping.")
    else:
        sys.exit("No opted in contacts. No contacts to send to Strive. Exiting.")

    return new_contacts

def randomize_participants(new_contacts):
    """
    Takes the downloaded new contacts and randomly sorts them into one of 3 trial groups.

    Group 1: Receives a Strive text
    Group 2: Control
    Group 3: Receives a Voicemail drop 
    """
    list_vanid = new_contacts['VanID'].tolist()
    random.shuffle(list_vanid)
    n = 3 # number of groups
    randomized_participants = np.array_split(list_vanid, 3)
   
    return randomized_participants

def combineDictList(*args):
    result = {}
    for dic in args:
        for key in (result.keys() | dic.keys()):
            if key in dic:
                result.setdefault(key, []).extend(dic[key])
    return result


def sort_participants(randomized_participants):

    group_strive = {"vanid": randomized_participants[0].tolist(), "participant_group" : ["Strive"] * len(randomized_participants[0]) }
    group_control = {"vanid" : randomized_participants[1].tolist(), "participant_group" : ["Control"] * len(randomized_participants[1])}
    group_voicemail = {"vanid" : randomized_participants[2].tolist(), "participant_group" : ["Voicemail"] * len(randomized_participants[2])}
    sorted_participants = combineDictList(group_strive, group_control)
    sorted_participants = combineDictList(sorted_participants, group_voicemail)


    return sorted_participants

def select_voicemail_participants(sorted_participants, new_contacts):
    sorted_participants_df = pd.DataFrame.from_dict(sorted_participants)
    voicemail_vanids = sorted_participants_df.loc[sorted_participants_df['participant_group'] == 'Voicemail']['vanid']
    voicemail_participants = new_contacts.loc[new_contacts['VanID'].isin(voicemail_vanids)]

    return voicemail_participants
    # Write voicemail group to CSV for email
    # voicemail_participants.to_csv('targets/group_voicemail.csv') 

def select_strive_participants(sorted_participants, new_contacts):
    sorted_participants_df = pd.DataFrame.from_dict(sorted_participants)
    strive_vanids = sorted_participants_df.loc[sorted_participants_df['participant_group'] == 'Strive']['vanid']
    strive_participants = new_contacts.loc[new_contacts['VanID'].isin(strive_vanids)]

    return strive_participants
def send_contacts_to_strive(strive_participants):
    """
    Takes the data frame from the `prepare_data` function and sends each contact
    to Strive and adds them to the "EA API member" group.
    """

    strive_headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + strive_key}

    for index, row in strive_participants.iterrows():
        phone_number = row['Phone']
        first_name = row['FirstName']
        if pd.isnull(first_name):
            first_name = "Friend"
        last_name = row['LastName']
        if pd.isnull(last_name):
            last_name = "Friend"
        payload = {
			    "phone_number": phone_number,
			    "campaign_id": campaign_id,
			    "first_name": first_name,
			    "last_name": last_name,
			    "opt_in": True,
			      "groups": [
			        {
			          "name": "Welcome Flow Experiment"
			        }
   				  ]
                }
        response = requests.request("POST", 'https://api.strivedigital.org/members', headers = strive_headers, data = json.dumps(payload))
        if response.status_code == 201:
        	logger.info(f"Successfully added: {first_name} {last_name}")
        else:
        	logger.info(f"Was not able to add {first_name} {last_name} to Stive. Error: {response.status_code}")

def send_email(voicemail_participants):
    message = Mail(
        from_email='brittany@sunrisemovement.org',
        to_emails='brittany@sunrisemovement.org',
        subject='Testing Automation of Voicemail Drop CSV',
        html_content='Here is your CSV')

    voicemail_participants.to_csv('group_voicemail.csv')
    with open('group_voicemail.csv', 'rb') as f:
        data = f.read()
        f.close()
    encoded_file = base64.b64encode(data).decode()

    attachedFile = Attachment(
        FileContent(encoded_file),
        FileName('group_voicemail.csv'),
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
                    select vanid from sunrise.welcome_flow_experiment_participants
                    """)

    new_vanids = pd.DataFrame.from_dict(sorted_participants)
    # Reorder dataframe with vanid in uniqueid spot
    new_vanids = new_vanids[['vanid', 'participant_group']]
    # Add created_at column
    new_vanids['created_at'] = date.today()
    # Remove existing vanids from new_vanids 
    new_vanids = new_vanids.loc[~new_vanids['vanid'].isin(existing_vanids)]
    # Convert dataframe to Parsons table for copy to Redshift 
    result_table = Table.from_dataframe(new_vanids)

    # copy Table into Redshift, append new rows
    rs.copy(result_table, 'sunrise.welcome_flow_experiment_participants' ,if_exists='append', distkey='vanid', sortkey = None, alter_table = True)

if __name__ == "__main__":
    logger.info("Initiate Export Job")
    everyaction_download_url = get_every_action_contacts(everyaction_headers, everyaction_auth)
    downloadLink = get_export_job(everyaction_download_url, everyaction_headers, everyaction_auth)
    new_contacts = prepare_data(downloadLink)
    randomized_participants = randomize_participants(new_contacts)
    sorted_participants = sort_participants(randomized_participants)
    strive_participants = select_strive_participants(sorted_participants, new_contacts)
    voicemail_participants = select_voicemail_participants(sorted_participants, new_contacts)
    send_contacts_to_strive(strive_participants)
    push_to_redshift(sorted_participants)
    send_email(voicemail_participants)
    
