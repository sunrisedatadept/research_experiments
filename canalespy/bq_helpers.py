from canalespy import logger
import datetime
import json

def get_project_from_credential(username):
    """
        usernames are formatted like:
        sa-tmc-mem-{member_code}@proj-tmc-mem-{member_code}.iam.gserviceaccount.com

        set username = os.environ['GOOGLE_APPLICATION_CREDENTIALS_USERNAME']
    """

    project = username.split('@')[1].split('.')[0]

    return project
    

def get_tmc_raw_row_count(db):

    schemas = db.query("""
        SELECT DISTINCT schema_name
        FROM `tmc-data-warehouse.INFORMATION_SCHEMA.SCHEMATA`
        WHERE schema_name like 'raw%'""")['schema_name']

    rows = 0
    for x in schemas:
        xrows = db.query(f"""
                    SELECT row_count
                    FROM `tmc-data-warehouse`.{x}.__TABLES__
                    """)
        if xrows:
            rows = rows + xrows.first

    return rows


def attempt_upsert(db, tbl, destination, uid):
    TIMESTAMP = datetime.datetime.now().strftime("%Y%m%d")
    
    # need to clone the ALL STRING format we have
    db.query(f"""
    DROP TABLE IF EXISTS {destination}_{TIMESTAMP}_new;
    CREATE TABLE {destination}_{TIMESTAMP}_new 
        CLONE {destination}
    """)
    db.copy(tbl, f"{destination}_{TIMESTAMP}_new", if_exists='truncate')
    
    #creating copy of existing table for safekeeping
    db.query(f"""
    DROP TABLE IF EXISTS {destination}_{TIMESTAMP}_old;
    CREATE TABLE {destination}_{TIMESTAMP}_old
        CLONE {destination}
    """)

    # updating destination with new tables
    db.query(f"""
    DELETE FROM {destination} 
        WHERE {uid} IN 
            (SELECT {uid}
            FROM {destination}_{TIMESTAMP}_new);

    INSERT INTO {destination} (
        SELECT * FROM {destination}_{TIMESTAMP}_new
    )
    """)

    # clean up old/new tables
    db.query(f"DROP TABLE {destination}_{TIMESTAMP}_old")
    db.query(f"DROP TABLE {destination}_{TIMESTAMP}_new")

    return None


def tbl_cols_list_to_json(tbl):
    """
        BigQuery does not like lists, but does like JSON!
    """

    list_cols = [x for x in tbl.columns if 'list' in tbl.get_column_types(x)]
    if len(list_cols) > 0:
        for x in list_cols:
            tbl.convert_column(x, lambda v: json.dumps(v) if v is not None else v)

    return tbl
