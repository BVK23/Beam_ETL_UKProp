import boto3
import apache_beam as beam
from apache_beam.io import ReadFromText
import re
from apache_beam.io.mongodbio import WriteToMongoDB
from apache_beam.options.pipeline_options import PipelineOptions, S3Options, GoogleCloudOptions

import urllib.parse

# Transforms:

def parse_csv_line(line):
    pattern = r'"([^"]*)"'
    columns = re.findall(pattern, line)
    return columns

#ParDo transform to generate key value pairs to later aggregate the data for each property

class ExtractInfo(beam.DoFn):
    def process(self, data):
        transaction_id = data[0].strip('"')
        price = int(data[1].strip('"'))
        date_of_transfer = data[2].strip('"')
        postcode = data[3].strip('"')
        property_type = data[4].strip('"')
        old_new = data[5].strip('"')
        duration = data[6].strip('"')
        paon = data[7].strip('"').replace(",", "")
        saon = data[8].strip('"').replace(",", "")
        street = data[9].strip('"').replace(",", "")
        locality = data[10].strip('"').replace(",", "")
        town_city = data[11].strip('"').replace(",", "")
        district = data[12].strip('"')
        county = data[13].strip('"')
        ppd_category_type = data[14].strip('"')
        record_status = data[15].strip('"')

        # Generate property key
        property_key = ','.join([paon, saon, street, locality, postcode.split(' ')[0]])

        return [(property_key, {
            'Tran_uid': transaction_id,
            'Price': price,
            'Date_tran': date_of_transfer,
            'Postcode': postcode,
            'PType': property_type,
            'Old_New': old_new,
            'Duration': duration,
            'Town_City': town_city,
            'District': district,
            'County': county,
            'PPD_cat': ppd_category_type,
            'Record_stat': record_status
        })]

#Generating unique property ID and formatting the grouped data to NDJSON format to be then stored in MongoDB Atlas

class FormatInfo(beam.DoFn):
    def __init__(self):
        self.db = None
        
    def setup(self):   
        import firebase_admin
        from firebase_admin import firestore
        
        if not firebase_admin._apps:
            firebase_admin.initialize_app()
        
        self.db = firestore.client()
  
    def process(self, data):
        p_key = data[0].split(',')[-1]
        
        if len(p_key) < 2:
            #Skipping transform for properties without postcode
            return
        
        else:    
            doc_ref = self.db.collection("property_id").document(p_key)
            doc = doc_ref.get()
            
            # Check if the document exists
            if doc.exists:
                data_in_doc = doc.to_dict()
                value = data_in_doc["value"] + 1
            else:
                value = 1

            num = str(value).zfill(4)
            property_id = p_key + '-' + num
        
            doc_ref.set({"value": value})
                    
            property_data = {
                'PropertyID': property_id,
                'Postcode': data[1][-1]['Postcode'],
                'PAON': data[0].split(',')[0],
                'SAON': data[0].split(',')[1],
                'Street': data[0].split(',')[2],
                'Locality': data[0].split(',')[3],
                'TownCity': data[1][-1]['Town_City'],
                'District': data[1][-1]['District'],
                'County': data[1][-1]['County']
            }
            transaction_data = []
            for transaction in data[1]:
                transaction_data.append({
                    'PropertyID': property_id,
                    'Tran_uid': transaction['Tran_uid'],
                    'Price': transaction['Price'],
                    'Date_Tran': transaction['Date_tran'],
                    'PType': transaction['PType'],
                    'Old_New': transaction['Old_New'],
                    'Duration': transaction['Duration'],
                    'PPD_cat': transaction['PPD_cat'],
                    'Record_stat': transaction['Record_stat']
                })

            yield property_data
            for transaction in transaction_data:
                yield transaction

#Composite Transform

class LoadToMongoDB(beam.PTransform):
    def __init__(self, connection_uri, db):
        self.connection_uri = connection_uri
        self.db = db

    def expand(self, pcollection):
        for filter_condition, coll in [('Street', 'birmingham_prop'), ('Tran_uid', 'birmingham_tran')]:
            _ = (pcollection
                 | f'Filtering for {filter_condition} in Data' >> beam.Filter(lambda x, condition=filter_condition: condition in x)
                 | f'Loading {coll} Data' >> WriteToMongoDB(uri=self.connection_uri, db=self.db, coll=coll))


"""***  ETL 1: To store historical data of Price Paid Data  ***"""

def main():
    
    # URL encode the username and password
    username = urllib.parse.quote_plus("USERNAME")
    password = urllib.parse.quote_plus("PASSWORD")  

    # Create the connection URI with the encoded username and password
    connection_uri = f"mongodb+srv://{username}:{password}@clustervk.ofeimsy.mongodb.net/"
        
    
    options = PipelineOptions()

    options_aws = options.view_as(S3Options)
    options_aws.s3_region_name = 'Your_S3_Region'  # Replace with your desired AWS region
    options_aws.s3_access_key_id = 'Your_Key_Id'  # Replace with your AWS access key ID
    options_aws.s3_secret_access_key = 'Your_Key' 
    
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'Your_Project_Id'  # Replace with your actual project ID
    google_cloud_options.job_name = 'etl1build2'  # Replace with your desired job name
    google_cloud_options.staging_location = 'gs://uk_property_registration/ETL1/stage_fold'  # Replace with your GCS staging bucket
    google_cloud_options.temp_location = 'gs://uk_property_registration/ETL1/temp_fold'  # Replace with your GCS temp bucket

    with beam.Pipeline(options=options) as p:
       
        lines = p | ReadFromText("s3://Your_bucket/pp-complete.csv")
        properties = (
            lines
            | 'Parse CSV Lines' >> beam.Map(parse_csv_line)
            | 'Filtering Data' >> beam.Filter(lambda x: x[12] == 'BIRMINGHAM') #Only a subset of the entire Data 
            | 'Key-Value Gen' >> beam.ParDo(ExtractInfo())
            | 'Aggregating the Data' >> beam.GroupByKey()
            | 'Formating the Data' >> beam.ParDo(FormatInfo())
            )
        
        properties | 'Load to MongoDB' >> LoadToMongoDB(connection_uri, db="bhampropertydata")


if __name__ == "__main__":
    main()   

