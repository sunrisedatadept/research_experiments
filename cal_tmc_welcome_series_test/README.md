# Welcome Email Test

Script written for a 2021 10 experiment in collaboration with TMC and Climate Advocacy Lab on our Welcome Series. 

* Uses the EA Changed Entities API to get the priors day new SMS opted in contacts
* Drops contacts that have the Activist Code `ExFromWelcomeSeries`
* Randomly sort the new contacts into one of three groups (Welcome Call, Wed Anytime Action, Friday Anytime Action)
* Sends the participants to Redshift for later analysis 
* Sends an email to Jasy and a volunteer to text via Spoke 

## Usage

1. Clone this Github repository -- you'll need to specify your new url in the civis interface
2. Create a new Container Script in Civis
3. The following parameters must be set in the script for this to work:

| PARAMETER NAME     | DISPLAY NAME       | DEFAULT | TYPE              | MAKE REQUIRED |
|--------------------|--------------------|---------|-------------------|---------------|
| REDSHIFT           | Redshift           | N/A     | Database          | Yes           |
| VAN                | VAN                | N/A     | Custom Credential | Yes           |
| AWS                | AWS                | N/A     | AWS Credential    | Yes           |


4. Connect civis to your github repository and point it appropriately.

5. Put the following in the command lines COMMAND section:

```
pip install pandas
python path/to/random_sample_processpy


```



