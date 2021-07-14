# Welcome Flow Text

Script written for a 2021 07 experiment in collaboration with TMC and Climate Advocacy Lab on our Welcome Flow. 

* Uses the EA Changed Entities API to get the priors day new SMS opted in contacts
* Randomly sort the new contacts into one of three groups
* Send 1/3 to a volunteer via email to upload to Spoke
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
| SEND_GRID          | Send Grid          | N/A     | Custom Credential | Yes           |

4. Connect civis to your github repository and point it appropriately.

5. Put the following in the command lines COMMAND section:

```
pip install pandas
pip install dictlib
pip install sendgrid
python path/to/random_sample_processpy


```



