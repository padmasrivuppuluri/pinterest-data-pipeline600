import requests
from time import sleep
import random
from multiprocessing import Process
import json
import sqlalchemy
from sqlalchemy import text
import yaml

"""
This script retrieves data from AWS RDS, formats it, and sends it to Kafka topics.
"""
random.seed(100)

pin_invoke_url = "https://b2ga3d1vdc.execute-api.us-east-1.amazonaws.com/dev/topics/0affec8c1897.pin"
geo_invoke_url =  "https://b2ga3d1vdc.execute-api.us-east-1.amazonaws.com/dev/topics/0affec8c1897.geo"
user_invoke_url = "https://b2ga3d1vdc.execute-api.us-east-1.amazonaws.com/dev/topics/0affec8c1897.user"  

                
class AWSDBConnector:
      
    def __init__(self,creds_file = 'db_creds.yaml'):
        self.creds_file = creds_file

    def read_db_creds(self):
        with open(self.creds_file, 'r') as f:
            data = yaml.safe_load(f)  # Load YAML file as a dictionary
        return data
        
    def create_db_connector(self):
        creds = self.read_db_creds()
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{creds['USER']}:{creds['PASSWORD']}@{creds['HOST']}:{creds['PORT']}/{creds['DATABASE']}?charset=utf8mb4")
        return engine

new_connector = AWSDBConnector()

def send_to_kafka(api_invoke_url, payload):
                headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
                response = requests.request("POST",api_invoke_url , headers=headers, data=payload)
                print(f'response code: {response.status_code}')
                print(f'content: {response.content}')

def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            #To send JSON messages we need to follow this structure
            payload_pin = json.dumps({
                "records":  [
                    {
                    #Data should be send as pairs of column_name:value, with different columns separated by commas    
                    "value": {"index": pin_result["index"],"unique_id": pin_result["unique_id"],"title": pin_result["title"],"description": pin_result["description"],
                            "poster_name": pin_result["poster_name"],"follower_count": pin_result["follower_count"],"tag_list": pin_result["tag_list"],
                            "is_image_or_video": pin_result["is_image_or_video"],"image_src": pin_result["image_src"],"downloaded": pin_result["downloaded"],
                            "save_location": pin_result["save_location"],"category": pin_result["category"]}
                            }
                ]
            })
            payload_geo =json.dumps({
                "records":  [
                    {"value":{"ind": geo_result["ind"],"timestamp": str(geo_result["timestamp"]),"latitude":geo_result["latitude"],"longitude":geo_result["longitude"],"country":geo_result["country"]} }
                ]
            })
            payload_user = json.dumps({
                "records":  [
                    {"value":{"ind": user_result["ind"],"first_name":user_result["first_name"],"last_name":user_result["last_name"],"age": user_result["age"],"date_joined": str(user_result["date_joined"])} }
                ]
            })

            send_to_kafka(pin_invoke_url, payload_pin)
            send_to_kafka(geo_invoke_url, payload_geo)
            send_to_kafka(user_invoke_url, payload_user)
            # print(pin_result)
            # print(geo_result)
            # print(user_result)
        #break

if __name__ == "__main__":
    run_infinite_post_data_loop()
    #print('Working')
    
    