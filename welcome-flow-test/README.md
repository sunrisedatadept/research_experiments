# Welcome Flow Text

Script written for a 2021 07 experiment in collaboration with TMC and Climate Advocacy Lab on our Welcome Flow. 

* Uses the EA Changed Entities API to get the priors day new SMS opted in contacts
* Randomly sort the new contacts into one of three groups
* Send 1/3 of the contacts to Strive for a welcome text
* Send 1/3 in a CSV to an email address that triggers a Zapier for a voicemail drop
* Sends the group assignments to our data warehouse (vanid + group + date)

## Usage

1. Clone this Github repository -- you'll need to specify your new url in the civis interface
2. Create a new Container Script in Civis
3. The following parameters must be set in the script for this to work:

| PARAMETER NAME     | DISPLAY NAME       | DEFAULT | TYPE              | MAKE REQUIRED |
|--------------------|--------------------|---------|-------------------|---------------|
| REDSHIFT           | Redshift           | N/A     | Database          | Yes           |
| VAN                | VAN                | N/A     | Custom Credential | Yes           |
| AWS                | AWS                | N/A     | AWS Credential    | Yes           |
| STRIVE             | Strive             | N/A     | Custom Credential | Yes           |
| STRIVE_CAMPAIGN_ID | Strive Campaign ID | N/A     | Integer           | Yes           |
| SEND_GRID          | Send Grid          | N/A     | Custom Credential | Yes           |

4. Connect civis to your github repository and point it appropriately.

5. Put the following in the command lines COMMAND section:

```
pip install pandas
pip install dictlib
pip install sendgrid
python path/to/random_sample_process_CONTAINER.py


```
## Strive Set Up

This script works by sending contacts to an Automation flow we've set up in Strive. **You must first send at least 1 contact to Strive via the API for this feature to be available**. We are taking advantage of the fact that Strive never sends a welcome text to the same person. 

1. Navigate to the Automation section on the left sidebar in your campaign.
2. Automations > API > Create API automation 
3. Name your automation 
4. Set the trigger to "New member is added via API"
5. Choose the Flow
6. Create your automation 


