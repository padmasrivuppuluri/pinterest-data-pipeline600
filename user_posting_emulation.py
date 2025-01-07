from time import sleep
import random
from multiprocessing import Process
import sqlalchemy
from sqlalchemy import text
import yaml

"""
This script retrieves data from AWS RDS, formats it
"""
random.seed(100)

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
print(new_connector.read_db_creds())


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
            
            print(pin_result)
            print(geo_result)
            print(user_result)
            break

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


