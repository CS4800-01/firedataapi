import sys
import json
import mysql.connector
from mysql.connector import errorcode


def fetch_all_records(dbconnect):
    dbconnect.execute("SELECT * FROM locations")
    myresult = dbconnect.fetchall()
    return myresult


def fetch_single_record(dbconnect, id):
    dbconnect.execute("SELECT * FROM locations WHERE id='%s'", [id])
    myresult = dbconnect.fetchall()
    return myresult


def get_locations_near(dbconnect, lat, long, n):
    dbconnect.execute(
        "SELECT DISTINCT a.*, ( 3959 * acos( cos( radians('35.9046545') ) * cos( radians( a.latitude ) ) * cos( radians( a.longitude ) - radians(%s) ) + sin( radians(%s) ) * sin( radians( a.latitude ) ) ) ) AS distance FROM location_test a WHERE (((SELECT enabled FROM location_test b WHERE id=a.parent_id LIMIT 1) != 1 OR (a.parent_id IS NULL OR a.parent_id='') AND enabled = 1)) HAVING distance < '100000' ORDER BY distance ASC LIMIT %s",
        (long, lat, n))
    myresult = dbconnect.fetchall()
    return myresult


def lambda_handler(event, context):
    action = event['stageVariables']['action']
    target = event['stageVariables']['target']
    output = "[]"
    cpp_lat_long = ['34.05580382234754', '-117.81919862863134']
    try:
        cnx = mysql.connector.connect(
            host="edwin-dev.com",
            user="cpp4800",
            password="backinthe90siwasinaveryfamoustvshow",
            database="cpp4800"
        )
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Invalid user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        # Connection success!
        dbcnx = cnx.cursor()
        # Implement SWITCH
        if action == "fetch":
            # Implement another SWITCH
            if target == "locations":  # ALL TENANTS
                results = fetch_all_records(dbcnx)
                output = json.dumps(results, default=str)  # Must specify default to JSONify Datetime Fields
            if target == "location" and 'location_id' in event['stageVariables']:  # SINGLE TENANT
                location_id = event['stageVariables']['location_id']
                location_id = int(location_id)  # MUST be INT
                results = fetch_single_record(dbcnx, location_id)
                output = json.dumps(results, default=str)  # Must specify default to JSONify Datetime Fields
            if target == "geo" and 'howmany' in event['stageVariables']:
                howmany = event['stageVariables']['howmany']
                results = get_locations_near(dbcnx, cpp_lat_long[0], cpp_lat_long[1], int(howmany))
                output = json.dumps(results, default=str)  # Must specify default to JSONify Datetime Fields
        cnx.close()  # Close the connection
        return {
            'statusCode': 200,
            'body': output,
        }


payload = {
    "stageVariables": {
    "action": "fetch",
    "target": "locations",
    "location_id": "2"
}}
output = lambda_handler(payload, None)
print(output)
