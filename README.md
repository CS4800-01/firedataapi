# firedataapi

###################################
# PREDICT
###################################
Endpoint:
https://ldrael3t7d.execute-api.us-west-2.amazonaws.com/predict/firedata

Expected Payload Body:
{"date": "11/22/2052", "rain30d": 1.1, "rain60d": 2.2, "rain90d": 3.3}


###################################
# FETCH SINGLE RECORD
###################################
Endpoint:
https://ldrael3t7d.execute-api.us-west-2.amazonaws.com/get_single_location/firedata

Expected Payload Body:
{"location_id": 5}

Expected Output:
[[5, 35.532556, -119.28179, "Kern"]]


###################################
# FETCH MULTIPLE RECORDS
###################################
Endpoint:
https://ldrael3t7d.execute-api.us-west-2.amazonaws.com/get_single_location/firedata

Expected Payload Body:
{"locations": [250, 251]}

Expected Output:
[[250, 39.210667, -122.168889, "Colusa"], [251, 34.112042, -117.1857, "San Bernardino"]]


###################################
# FETCH ALL LOCATIONS
###################################
Endpoint:
https://ldrael3t7d.execute-api.us-west-2.amazonaws.com/get_all_locations/firedata

Expected Payload Body:
{"location_id": "all"}

Expected Output:
[[2, 36.336222, -120.11291, "Fresno"], [5, 35.532556, -119.28179, "Kern"], ...]


###################################
# FETCH LOCATION ID LIST
###################################
Endpoint:
https://ldrael3t7d.execute-api.us-west-2.amazonaws.com/get_location_id_list/firedata

Expected Payload Body:
{"location_id": "all"}

Expected Output:
[2, 5, 6, 7, 8, 12, 13, 15, 19, 21, 32, 35, ...]


###################################
# ADD RECORD
###################################
Endpoint:
https://ldrael3t7d.execute-api.us-west-2.amazonaws.com/add_location/firedata

Expected Payload Body:
{"records": [[111.111, -111.111, "Station 111"  ], [222.222, -222.222, "Station 222"]]}

Expected Output:
"2 records inserted. Last row: 270"


###################################
# DELETE RECORDS
###################################
Endpoint:
https://ldrael3t7d.execute-api.us-west-2.amazonaws.com/delete_location/firedata

Expected Payload Body:
{"locations": [269, 270]}

Expected Output:
"Deleted 2 records."