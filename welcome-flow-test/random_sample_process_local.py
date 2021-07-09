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
import ipdb
import dictlib
import numpy as np

# Set parameters
delta  = timedelta(hours =  24) ## Set this to the frequency of your Container Script

# Set local environmental variables
van_key = os.environ['VAN_API_KEY']
strive_key = os.environ['STRIVE_KEY']
campaign_id = os.environ['STRIVE_CAMPAIGN_ID']

# Set EA API credentials
username = 'welcometext'  ## This can be anything
db_mode = '1'    ## Specifying the NGP side of VAN
password = f'{van_key}|{db_mode}' ## Create the password from the key and the mode combined
everyaction_auth = HTTPBasicAuth(username, password)
everyaction_headers = {"headers" : "application/json"}

# Local environmental variables
os.environ['REDSHIFT_PORT']
os.environ['REDSHIFT_DB']
os.environ['REDSHIFT_HOST']
os.environ['REDSHIFT_USERNAME']
os.environ['REDSHIFT_PASSWORD']
os.environ['S3_TEMP_BUCKET']
os.environ['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']

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
    groups = np.array_split(list_vanid, 3)
   
    return groups

def combineDictList(*args):
    result = {}
    for dic in args:
        for key in (result.keys() | dic.keys()):
            if key in dic:
                result.setdefault(key, []).extend(dic[key])
    return result


def sort_participants(groups):

    group_strive = {"vanid": groups[0].tolist(), "group" : ["Strive"] * len(groups[0]) }
    group_control = {"vanid" : groups[1].tolist(), "group" : ["Control"] * len(groups[1])}
    group_voicemail = {"vanid" : groups[2].tolist(), "group" : ["Voicemail"] * len(groups[2])}
    ipdb.set_trace()
    participant_groups = combineDictList(group_strive, group_control)
    participant_groups = combineDictList(participant_groups, group_voicemail)

    return participant_groups

def send_contacts_to_strive(df_for_strive):
    """
    Takes the data frame from the `prepare_data` function and sends each contact
    to Strive and adds them to the "EA API member" group.
    """

    strive_headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + strive_key}

    for index, row in df_for_strive.iterrows():
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

def push_to_redshift(participant_groups):
    """
    Take the participant grouping and push to Redshift. 
    """

    results = pd.DataFrame.from_dict(participant_groups)
    results = results[['vanid', 'group']]
    result_table = Table.from_dataframe(results)

    # copy Table into Redshift, append new rows
    rs.copy(result_table, 'sunrise.welcome_flow_experiment_participants' ,if_exists='append', distkey='vanid', sortkey = None, alter_table = True)

if __name__ == "__main__":
    logger.info("Initiate Export Job")
    everyaction_download_url = get_every_action_contacts(everyaction_headers, everyaction_auth)
    downloadLink = get_export_job(everyaction_download_url, everyaction_headers, everyaction_auth)
    new_contacts = prepare_data(downloadLink)
    groups = randomize_participants(new_contacts)
    participant_groups = sort_participants(groups)
    #send_contacts_to_strive(group_strive)
    push_to_redshift(participant_groups)
    
