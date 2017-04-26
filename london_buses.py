from collections import defaultdict
import requests, json, os, time, bisect, math
from requests.auth import HTTPDigestAuth
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from datetime import datetime
import time
import uuid

num_retries=5

#connect to bigquery
#  Grab the application's default credentials from the environment.
credentials = GoogleCredentials.get_application_default()

# Construct the service object for interacting with the BigQuery API.
bigquery = discovery.build('bigquery', 'v2', credentials=credentials)

#function to stream a new row of data to bigquery
def stream_row_to_bigquery(bigquery, project_id, dataset_id, table_name, formatted_rows,
                           num_retries=3):

    insert_all_data = {'rows': formatted_rows}
    return bigquery.tabledata().insertAll(
        projectId=project_id,
        datasetId=dataset_id,
        tableId=table_name,
        body=insert_all_data).execute(num_retries=num_retries)


#STREAM_BUS_FEED = "http://countdown.api.tfl.gov.uk/interfaces/ura/stream_V1?ReturnList=StopPointName,StopID,StopCode1,StopCode2,StopPointType,Towards,Bearing,StopPointIndicator,StopPointState,Latitude,Longitude,VisitNumber,LineID,LineName,DirectionID,DestinationText,DestinationName,VehicleID,TripID,RegistrationNumber,EstimatedTime,ExpireTime,MessageUUID,MessageType,MessagePriority,MessageText,StartTime"
STREAM_BUS_FEED = "http://countdown.api.tfl.gov.uk/interfaces/ura/stream_V1?ReturnList=StopPointName,StopID,StopCode1,StopCode2,StopPointState,StopPointType,StopPointIndicator,Towards,Bearing,Latitude,Longitude,VisitNumber,TripID,VehicleID,RegistrationNumber,LineID,LineName,DirectionID,DestinationText,DestinationName,EstimatedTime,MessageUUID,MessageText,MessageType,MessagePriority,StartTime,ExpireTime,BaseVersion"

#STREAM_BUS_FEED = 'http://countdown.api.tfl.gov.uk/interfaces/ura/stream_V1'
STREAM_USERNAME = '<USERNAME>'
STREAM_PASSWORD = '<PASSWORD>'
PREDICTION_RESPONSE_TYPE=1
COMMIT_SIZE=500

def Stream():
    try:
        formatted_rows=[]
        r=requests.get(STREAM_BUS_FEED, auth=HTTPDigestAuth(STREAM_USERNAME, STREAM_PASSWORD), stream=True)
        for line in r.iter_lines():
            if not line:
                continue
            j = json.loads(line)
            row = {}

            if j[0]==1: #if responseType=1
                row['ResponseType'] = int(j[0])
                row['StopPointName'] = j[1]
                row['StopID'] = j[2]
                row['StopCode1'] = j[3]
                row['StopCode2'] = j[4]
                row['StopPointType'] = j[5]
                row['Towards'] = j[6]
                row['Bearing'] = int(j[7])
                row['StopPointIndicator'] = j[8]
                row['StopPointState'] = int(j[9])
                row['Latitude'] = float(j[10])
                row['Longitude'] = float(j[11])
                row['VisitNumber'] = int(j[12])
                row['LineID'] = j[13]
                row['LineName'] = j[14]
                row['DirectionID'] = int(j[15])
                row['DestinationText'] = j[16]
                row['DestinationName'] = j[17]
                row['VehicleID'] = str (j[18])
                row['TripID'] = int(j[19])
                row['RegistrationNumber'] = j[20]
                row['EstimatedTime'] = float(j[21])/1000
                row['ExpireTime'] = float(j[22])/1000

                formatted_rows.append({
                    'json': row,
                    # Generate a unique id for each row so retries don't accidentally duplicate insert
                    'insertId': str(uuid.uuid4()),
                })

                if len(formatted_rows)==COMMIT_SIZE:
                    # insert data into database
                    response=stream_row_to_bigquery(
                        bigquery, '<PROJECT NAME>', '<DATASET NAME>', '<TABLE NAME>', formatted_rows, num_retries)
                    print(row)
                    print(response)
                    formatted_rows=[]

    except Exception as e:
        print(e)
        Stream()

while True:
    try:
        # main program
        Stream()

    except:
        # Oh well, reconnect and keep going
        continue

