#imports
import boto3
import pandas as pd
import time
from datetime import datetime
from botocore.config import Config

#constants
DATABASE_NAME = "spinalytics"
TABLE_NAME = "songs"

class Ingestion:

    def __init__(self, client):
        self.client = client
    
    def datetime_to_unix(self, songs):
        datetime_str = songs["date"] + " " + songs["timestamp"]
        datetime_obj = datetime.strptime(datetime_str, "%b %d, %Y %I:%M %p")
        print(datetime_obj)
        print(datetime_obj.timestamp())
        return int(datetime_obj.timestamp())
    
    def submit(self, records, counter):
        try:
            result = self.client.write_records(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME, Records=records, CommonAttributes={})
            print("Processed [%d] records. WriteRecords Status: [%s]" % (counter, result['ResponseMetadata']['HTTPStatusCode']))
        except Exception as err:
            print("Error:", err)
            rejected_records = err.response['RejectedRecords']  
            print(f"Rejected records: {rejected_records}")  

    def ingest(self, filepath):
        #read csv
        songs = pd.read_csv(filepath)

        

        #convert dates/times to unix epoch format
        songs["unix"] = songs.apply(self.datetime_to_unix, axis=1)

        #sort df in chronological order
        songs = songs.sort_values(by="unix")
        
        #find duration of each song
        songs['next_song_time'] = songs['unix'].shift(-1)
        songs["duration"] = songs['next_song_time'] - songs['unix']
        songs['duration'].fillna(180, inplace=True)

        records = []
       
        counter = 0

        for _, j in songs.iterrows():

            dimensions = [
                {"Name": "Artist", "Value": j[0]},
                {"Name": "Song", "Value": j[6]},
                {"Name": "Genre", "Value": j[3]},
                {"Name": "Show_Name", "Value": j[4]},
                {"Name": "DJ", "Value": j[2]},
                {"Name": "Date", "Value": j[1]},
                {"Name": "TimePM", "Value": j[5]}

            ]
            

            
            record = {
                "Dimensions": dimensions,
                "MeasureName": "Song_Duration",
                "MeasureValue" : str(j[9]/60),
                "MeasureValueType": "DOUBLE",
                "Time": str(j[7]*1000)
            }
            
            records.append(record)
            counter = counter + 1

            if len(records) == 100:
                self.submit(records, counter)
                records = []
        
        if len(records) != 0:
            self.submit(records, counter)

        print("Ingested %d records" % counter)

def main():
    session = boto3.Session()
    write_client = session.client('timestream-write', config=Config(region_name="us-east-2", read_timeout=20, max_pool_connections = 5000, retries={'max_attempts': 10}))
    query_client = session.client('timestream-query', config=Config(region_name="us-east-2"))
    csv_ingestion_example = Ingestion(write_client)
    csv_ingestion_example.ingest("locked_groove.csv")
        

main()

