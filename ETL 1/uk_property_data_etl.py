# -*- coding: utf-8 -*-
"""

# **Data Engineering ETL: HM Land Registry Price Paid Data**

**Install neccesary libraries**
"""

# !pip install boto3==1.26.90

# !pip install s3fs

# !pip install apache_beam

"""

 **Code blocks begin**


"""

import boto3
import apache_beam as beam
import json
from apache_beam.io import ReadFromText
import re
import urllib.parse

from apache_beam.io.mongodbio import WriteToMongoDB
import pymongo
import pickle
from apache_beam.options.pipeline_options import PipelineOptions

#Importing all the neccesary libraries

import urllib.parse

# URL encode the username and password
username = urllib.parse.quote_plus("USERNAME")
password = urllib.parse.quote_plus("PASSWORD")

# Create the connection URI with the encoded username and password
connection_uri = f"mongodb+srv://{username}:{password}@clustervk.ofeimsy.mongodb.net/"

#You can fetch this connection string from MongoDB Atlas or your local Compass DB


"""Initialising pipeline options for reading the Tranactions data csv file stored on AWS S3 Bucket"""

# options_aws = pipeline_options.S3Options([
#                "--s3_region_name=",
#                 "--s3_access_key_id=",
#                 "--s3_secret_access_key="
#             ])
 #Pipeline options to connect to S3 bucket and Extract csv data

"""# Transforms

Transform to split indivual fields of the data
"""

def parse_csv_line(line):
    pattern = r'"([^"]*)"'
    columns = re.findall(pattern, line)
    return columns

#Some on the columns have comma in them, forcing us to use regular expression to parse instead of split() method

"""ParDo transform to generate key value pairs to later aggregate the data for each property"""

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

        #This is the best logic to generate key for identifiying properties uniquely. 

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

"""Based on running tests and analysing the Dataset, i concluded to uniquely identify each property based on the combition of PAON,SAON,Street,Locality and first half of the postcode. More info: check Repo Readme"""



"""Initialising a Dictionary to generate property ID"""

dict_post={}

"""I decided to use the first half of the post code for generating unique property ID. for each of these codes, count of property was and appended to give the final Property ID  

The below function describes the process:
"""

def propkeyid_gen(postcode):
    global dict_post
    split_c = postcode
    
    if split_c in dict_post:
        dict_post[split_c] += 1
    else:
        dict_post[split_c] = 1
    
    count_postc = dict_post[split_c]
    num = str(count_postc).zfill(4)
    
    property_key=split_c+'-'+num
    return property_key

#We are using the first half of postcode for generating the property key


"""Formatting the grouped data to NDJSON format to be then stored in MongoDB Atlas"""

class FormatInfo(beam.DoFn):
    def process(self, data):

        property_id = propkeyid_gen(data[0].split(',')[-1])  # Generate property ID
        
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
        for filter_condition, coll in [('PAON', 'bham_prop'), ('Tran_uid', 'bham_tran')]:
            _ = (pcollection
                 | beam.Filter(lambda x: filter_condition in x)
                 | f'Loading {coll} Data' >> WriteToMongoDB(uri=self.connection_uri, db=self.db, coll=coll))


"""***  ETL 1: To store historical data of Price Paid Data  ***"""

def main():
    
    #best pipeline options for uisng DirectRunner on my PC

    options = PipelineOptions(
        runner='DirectRunner',
        num_workers=8
    )

    with beam.Pipeline(options=options) as p:
       
        lines = p | ReadFromText('../../Downloads/pp-complete.csv')
        properties = (
            lines
            | 'Parse CSV Lines' >> beam.Map(parse_csv_line)
            | 'Filtering Data' >> beam.Filter(lambda x: x[13] == 'WEST MIDLANDS') #Only a subset of the entire Data 
            | 'Key-Value Gen' >> beam.ParDo(ExtractInfo())
            | 'Aggregating the Data' >> beam.GroupByKey()
            | 'Formating the Data' >> beam.ParDo(FormatInfo())
            )
        
        properties | 'Load to MongoDB' >> LoadToMongoDB(connection_uri, db="propertyuk")


    postcode_output_path = 'postcode_dictionary_wm.pkl'

    # Store the Postcode dictionary as a pickle file
    with open(postcode_output_path, 'wb') as f:
        pickle.dump(dict_post, f) 


if __name__ == "__main__":
    main()   

