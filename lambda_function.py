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


def fetch_multiple_records(dbconnect, id_list):
    implode = ','.join(['%s'] * len(id_list))
    dbconnect.execute("SELECT * FROM locations WHERE id IN (%s)" % implode, tuple(id_list))
    myresult = dbconnect.fetchall()
    return myresult


def new_record(dbconnect, record_list):
    query = "INSERT INTO locations (`lat`, `long`, `name`) VALUES " + ",".join("(%s, %s, %s)" for _ in record_list)
    flattened_values = [item for sublist in record_list for item in sublist]
    dbconnect.execute(query, flattened_values)
    myresult = str(dbconnect.lastrowid)
    insCount = str(len(record_list))
    ans = insCount + " records inserted. Last row: " + myresult
    return ans


def delete_records(dbconnect, id_list):
    implode = ','.join(['%s'] * len(id_list))
    dbconnect.execute("DELETE FROM locations WHERE id IN (%s)" % implode, tuple(id_list))
    delCount = str(len(id_list))
    return "Deleted " + delCount + " records."


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
            if target == "all_locations":
                results = fetch_all_records(dbcnx)
                output = json.dumps(results, default=str)  # Must specify default to JSONify Datetime Fields
            if target == "single_location" and 'location_id' in event['stageVariables']:  # SINGLE TENANT
                location_id = event['stageVariables']['location_id']
                location_id = int(location_id)  # MUST be INT
                results = fetch_single_record(dbcnx, location_id)
                output = json.dumps(results, default=str)
            if target == "multiple_locations" and 'location_id_list' in event['stageVariables']:
                location_id_list = event['stageVariables']['location_id_list']
                results = fetch_multiple_records(dbcnx, location_id_list)
                output = json.dumps(results, default=str)
            if target == "geo" and 'howmany' in event['stageVariables']:
                howmany = event['stageVariables']['howmany']
                results = get_locations_near(dbcnx, cpp_lat_long[0], cpp_lat_long[1], int(howmany))
                output = json.dumps(results, default=str)
        elif action == "delete" and 'location_id_list' in event['stageVariables']:
            location_id_list = event['stageVariables']['location_id_list']
            results = delete_records(dbcnx, location_id_list)
            cnx.commit()
            output = json.dumps(results, default=str)
        elif action == "add" and "records" in event['stageVariables']:
            recordset = [tuple(x) for x in payload['stageVariables']['records']]
            results = new_record(dbcnx, recordset)
            cnx.commit()
            output = results
        cnx.close()  # Close the connection
        return {
            'statusCode': 200,
            'body': output,
        }


payload = {
    "stageVariables": {
        "action": "fetch",
        "target": "multiple_locations",
        "location_id": 5,
        "location_id_list": [250, 251],
        "records": [["111", "-111", "place1"], ["222", "-222", "place2"]]
    }}
print("Payload: ", payload)

output = lambda_handler(payload, None)
print(output)
