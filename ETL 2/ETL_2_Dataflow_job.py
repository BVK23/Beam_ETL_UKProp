import boto3
import apache_beam as beam
import re
import urllib.parse
from apache_beam.io.mongodbio import ReadFromMongoDB, WriteToMongoDB
import json
import pickle
import gcsfs
from apache_beam.options.pipeline_options import PipelineOptions, S3Options, GoogleCloudOptions
import ast


username = urllib.parse.quote_plus("BVK97")
password = urllib.parse.quote_plus("Spiderman@1997")

# Create the connection URI with the encoded username and password
connection_uri = f"mongodb+srv://{username}:{password}@clustervk.ofeimsy.mongodb.net/"



def parse_csv_line(line):
    #To parse data in the format {F87E72FA-022C-176C-E053-6B04A8C0D2BE},150000,2023-02-23 00:00,"Postcode",and rest in between double qoutes
    pattern = r'"([^"]+)"|([^,]+)'
    matches = re.findall(pattern, line)
    return [match[0] if match[0] else match[1] for match in matches]


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

        return [{"key":property_key, "value":{
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
        }}]



#List to store postcodes from incoming file to filter PropertyID fetch data

def store_postcodes(element):
    # Store values in the global list
    return element["value"]['Postcode']


class FormatInfo(beam.DoFn):
    def __init__(self, flag):
        self.flag = flag
        self.db = None
        
    def setup(self):   
        import firebase_admin
        from firebase_admin import firestore
        
        if not firebase_admin._apps:
            firebase_admin.initialize_app()
        
        self.db = firestore.client()
  
    def process(self, data):
        if self.flag:
                        
            p_key=data[0].split(',')[-1]
            if len(p_key) < 2:
            #Skipping transform for properties without postcode
                return 
        
            else:     
                doc_ref = self.db.collection("property_id").document(p_key) #property_id
                doc = doc_ref.get()

                # Check if the document exists
                if doc.exists:
                    data_in_doc = doc.to_dict()
                    value= data_in_doc["value"] + 1
                    
                else:
                    value = 1

                num = str(value).zfill(4)
                property_id = p_key + '-' + num
                
                doc_ref.set({"value": value})
                        
                property_data = {
                    'PropertyID': property_id,
                    'Postcode': data[1]["transactions"][-1]['Postcode'],
                    'PAON': data[0].split(',')[0],
                    'SAON': data[0].split(',')[1],
                    'Street': data[0].split(',')[2],
                    'Locality': data[0].split(',')[3],
                    'TownCity': data[1]["transactions"][-1]['Town_City'],
                    'District': data[1]["transactions"][-1]['District'],
                    'County': data[1]["transactions"][-1]['County']
                }
                yield property_data
        else:
            property_id = data[1]["propid_tuples"][-1]

        transaction_data = []
        for transaction in data[1]["transactions"]:
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

        for transaction in transaction_data:
            yield transaction


def main():
    
    options = PipelineOptions()

    options_aws = options.view_as(S3Options)
    options_aws.s3_region_name = 'eu-west-2'  # Replace with your desired AWS region
    options_aws.s3_access_key_id = 'AKIAQ76BX2HNMVPDHWZQ'  # Replace with your AWS access key ID
    options_aws.s3_secret_access_key = '3UKrzFUOwKzTQJxbzQCdc4jKZzSbpWhEyB/tAlG8' 
    
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'abeam-386113'  # Replace with your actual project ID
    google_cloud_options.job_name = 'etl2build1p1'  # Replace with your desired job name
    google_cloud_options.staging_location = 'gs://uk_property_registration/ETL2/stage_fold'  # Replace with your GCS staging bucket
    google_cloud_options.temp_location = 'gs://uk_property_registration/ETL2/temp_fold'  # Replace with your GCS temp bucket

    # Pipeline 1
    with beam.Pipeline(options=options) as pipeline1:
        # Read the new data; transaction-level
        transactions = (
                pipeline1
                | 'Read Transaction Data' >> beam.io.ReadFromText("s3://bvkawsbucket/pp-monthly-update_april_2023.txt")
                | 'Parse CSV Lines' >> beam.Map(parse_csv_line)
                | 'ETL for a subset' >>beam.Filter(lambda x: x[12] == 'BIRMINGHAM')
                      )

        transformed_transactions = (
                transactions
                | 'Transform Transaction Data' >> beam.ParDo(ExtractInfo())
        )
        store_temp_data = (
                transformed_transactions 
                | 'JSON Formatting' >> beam.Map(json.dumps)  
                | 'Write to Temp file' >> beam.io.WriteToText("gs://uk_property_registration/ETL2/temp_fold/pipe1_data", file_name_suffix="")
        )
        store_post_data = (
                transformed_transactions 
                |'Storing Postcodes of this batch' >>  beam.Map(store_postcodes)
                | beam.combiners.ToList()
                | 'Postcodes in a file' >> beam.io.WriteToText("gs://uk_property_registration/ETL2/list_postcodes",file_name_suffix="")
        )  

    # Pipeline 2
    filter_criteria = {"Postcode": {"$in": [postcode.strip("'") for postcode in ast.literal_eval(gcsfs.GCSFileSystem().open('gs://uk_property_registration/ETL2/list_postcodes-00000-of-00001').read().decode())]}}
    
          
    options2 = PipelineOptions()

    google_cloud_options2 = options2.view_as(GoogleCloudOptions)
    google_cloud_options2.project = 'abeam-386113'  # Replace with your actual project ID
    google_cloud_options2.job_name = 'etl2build1p2'  # Replace with your desired job name
    google_cloud_options2.staging_location = 'gs://uk_property_registration/ETL2/stage_fold'  # Replace with your GCS staging bucket
    google_cloud_options2.temp_location = 'gs://uk_property_registration/ETL2/temp_fold'

    with beam.Pipeline(options=options2) as pipeline2:
        # Read the transformed new data generated by pipeline1
        tuple_collection = (
                pipeline2
                | 'Temp File read' >> beam.io.ReadFromText("gs://uk_property_registration/ETL2/temp_fold/pipe1_data-00000-of-00001")
                | beam.Map(lambda x: json.loads(x))
                | 'Key, value(Transaction level data) pairs' >> beam.Map(lambda x: (x["key"], x["value"]))
        )

        prop_collection = (
                pipeline2
                | 'Reading existing Property data' >> ReadFromMongoDB(uri=connection_uri, db="propertyuk", coll="bham_prop", filter=filter_criteria,
                                  bucket_auto=True)
                | 'Key, value(Property ID) pairs' >> beam.Map(lambda x: (','.join([x['PAON'], x['SAON'], x['Street'], x['Locality'],
                                                x['Postcode'].split(' ')[0]]), x['PropertyID']))
                                                
        )
       
        combined_data = (
                {'transactions': tuple_collection, 'propid_tuples': prop_collection}
                | "Combine Data" >> beam.CoGroupByKey()
                | 'Filtering out Data' >> beam.Filter(lambda x: len(x[1]["transactions"]) > 0)
        )

        prop_w_key = (
                combined_data
                | 'For Data with matching Property ID' >> beam.Filter(lambda x: len(x[1]["propid_tuples"]) > 0)
                | 'Formating Data with ID' >> beam.ParDo(FormatInfo(False))
                | 'Transaction Data store 1' >> WriteToMongoDB(uri=connection_uri, db="bhampropertydata", coll="birmingham_tran")

        )

        prop_wo_key = (
                combined_data
                | 'For Data without matching Property ID' >> beam.Filter(lambda x: len(x[1]["propid_tuples"]) == 0)
                | 'Formating Data without Property ID' >> beam.ParDo(FormatInfo(True))
        )
        final_dump_tran = (
                prop_wo_key
                | 'For new Transaction Data' >> beam.Filter(lambda x: 'Tran_uid' in x)
                | 'Transaction Data store 2' >> WriteToMongoDB(uri=connection_uri, db="bhampropertydata", coll="birmingham_tran")
        )

        final_dump_prop = (
                prop_wo_key
                | 'For new Property Data' >> beam.Filter(lambda x: 'Tran_uid' not in x)
                | 'Property Data store' >> WriteToMongoDB(uri=connection_uri, db="bhampropertydata", coll="birmingham_prop")
        )
       


if __name__ == "__main__":
    main()



 