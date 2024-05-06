import os
import json
from parsons.tools.credential_tools import decode_credential

TMC_CIVIS_DATABASE = 815
TMC_CIVIS_DATABASE_NAME = "TMC"
# Check to see if an S3 bucket has been provided in environment variables.
# If not, use the parsons-tmc S3 bucket.
S3_BUCKET = os.environ["S3_BUCKET"] if "S3_BUCKET" in os.environ else "parsons-tmc"
GCS_BUCKET = (
    os.environ["GCS_BUCKET"] if "GCS_BUCKET" in os.environ else "tmc-dev-scratch"
)


def set_env_var(name, value, overwrite=False):
    """
    Set an environment variable to a value.
    `Args:`
        name: str
            Name of the env var
        value: str
            New value for the env var
        overwrite: bool
            Whether to set the env var even if it already exists
    """
    # Do nothing if we don't have a value
    if not value:
        return

    # Do nothing if env var already exists
    if os.environ.get(name) and not overwrite:
        return

    os.environ[name] = value


def setup_environment(
    redshift_parameter="REDSHIFT",
    aws_parameter="AWS",
    copper_parameter="COPPER",
    google_sheets_parameter="GOOGLE_SHEETS",
    google_application_parameter="GOOGLE_APPLICATION_CREDENTIALS",
    newmode_api_parameter="NEWMODE_API",
    airtable_personal_access_token_parameter="AIRTABLE_PERSONAL_ACCESS_TOKEN",
    member_project=False,
):
    """
    Sets up environment variables needed for various common services used by our scripts.
    Call this at the beginning of your script.
    `Args:`
        redshift_parameter: str
            Name of the Civis script parameter holding Redshift credentials. This parameter
            should be of type "database (2 dropdown)" in Civis.
        aws_parameter: str
            Name of the Civis script parameter holding AWS credentials.
        copper_parameter: str
            Name of the Copper script parameter holding Copper user email and API key
        google_sheets_parameter: str
            Name of the Google Sheets script parameter holding a base64-encoded Google
            credentials dict
        airtable_personal_access_token_parameter: str
            Name of the airtable personal access token credential holding the api token credential
        member_project: bool
            Obsolete
    """

    env = os.environ

    # Civis setup

    set_env_var("CIVIS_DATABASE", str(TMC_CIVIS_DATABASE))

    # Redshift setup

    set_env_var("REDSHIFT_PORT", "5432")
    set_env_var("REDSHIFT_DB", "dev")
    set_env_var("REDSHIFT_HOST", env.get(f"{redshift_parameter}_HOST"))
    set_env_var(
        "REDSHIFT_USERNAME", env.get(f"{redshift_parameter}_CREDENTIAL_USERNAME")
    )
    set_env_var(
        "REDSHIFT_PASSWORD", env.get(f"{redshift_parameter}_CREDENTIAL_PASSWORD")
    )

    # AWS setup

    set_env_var("S3_TEMP_BUCKET", str(S3_BUCKET))
    set_env_var("AWS_ACCESS_KEY_ID", env.get(f"{aws_parameter}_USERNAME"))
    set_env_var("AWS_SECRET_ACCESS_KEY", env.get(f"{aws_parameter}_PASSWORD"))

    # Copper setup

    set_env_var("COPPER_USER_EMAIL", env.get(f"{copper_parameter}_USERNAME"))
    set_env_var("COPPER_API_KEY", env.get(f"{copper_parameter}_PASSWORD"))

    # Google Sheets setup

    if f"{google_sheets_parameter}_PASSWORD" in env:
        key_dict = decode_credential(
            env.get(f"{google_sheets_parameter}_PASSWORD"), export=False
        )
        key_json = json.dumps(key_dict)
        set_env_var("GOOGLE_DRIVE_CREDENTIALS", key_json)

    # BigQuery setup
    if f"{google_application_parameter}_PASSWORD" in env:
        json_format_app_cred = json.loads(
            env.get(f"{google_application_parameter}_PASSWORD"), strict=False
        )
        set_env_var("GOOGLE_APPLICATION_CREDENTIALS", json.dumps(json_format_app_cred))

    # GCS_TEMP_BUCKET set
    # If GCS bucket env variable wasn't included, we're going to construct it
    gcs_bucket = GCS_BUCKET
    if (
        not os.getenv("GCS_TEMP_BUCKET")
        and not os.getenv("GCS_BUCKET")
        and os.getenv("CIVIS_RUN_ID")
        and os.getenv(f"{google_application_parameter}_USERNAME")
    ):
        # If member service account, then use membercode from there
        service_account = os.environ[f"{google_application_parameter}_USERNAME"]
        if service_account[:10] == "sa-tmc-mem":
            member_code = service_account.split("@")[0].split("-")[-1]
            gcs_bucket = f"bkt-tmc-mem-{member_code.lower()}-scratch"
        # If DEV credential, use dev bucket, otherwise use prod bucket
        elif "dev" in service_account.lower():
            gcs_bucket = "tmc-dev-scratch"
        else:
            gcs_bucket = "tmc-scratch"

    set_env_var("GCS_TEMP_BUCKET", gcs_bucket)

    # Airtable setup
    set_env_var(
        "AIRTABLE_PERSONAL_ACCESS_TOKEN",
        env.get(f"{airtable_personal_access_token_parameter}_PASSWORD"),
    )

    # New/Mode setup

    set_env_var("NEWMODE_API_USER", env.get(f"{newmode_api_parameter}_USERNAME"))

    # Parsons UI Setup

    if "DIALECT" in env:
        dialect = env["DIALECT"].lower()
        # For Redshift and MySQL, the pattern DIALECT_KEY, eg REDSHIFT_USERNAME or
        # MYSQL_PASSWORD
        if dialect in ["redshift", "mysql"]:
            prefix = f'{env["DIALECT"].upper()}'
            for key in ["DB", "HOST", "USERNAME", "PASSWORD", "PORT"]:
                if key in env:
                    new_key = f"{prefix}_{key}"
                    set_env_var(new_key, env[key])
        # Postgres has its own scheme for naming the environment variables
        elif dialect == "postgres":
            set_env_var("PGUSER", env["USERNAME"])
            set_env_var("PGPASS", env["PASSWORD"])
            set_env_var("PGHOST", env["HOST"])
            set_env_var("PGPORT", env["PORT"])
            set_env_var("PGDATBASE", env["DB"])
        # If it's not Redshift or MySQL or Postgres, we probably aren't going to
        # be able to handle it
        else:
            raise ValueError(f"Unknown dialect: {dialect}")
