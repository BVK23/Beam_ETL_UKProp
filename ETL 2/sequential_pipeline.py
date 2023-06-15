import apache_beam as beam
import os
from apache_beam.io import ReadFromText
import re
import urllib.parse
from apache_beam.options import pipeline_options
from apache_beam.io.mongodbio import ReadFromMongoDB,WriteToMongoDB
import json
import pickle

username = urllib.parse.quote_plus("YOUR_USERNAME")
password = urllib.parse.quote_plus("YOUR_PASSWORD")

# Create the connection URI with the encoded username and password
connection_uri = f"mongodb+srv://{username}:{password}@clustervk.ofeimsy.mongodb.net/"

options_aws = pipeline_options.S3Options([
                "--s3_region_name=<your_region>",
                "--s3_access_key_id=<your_key_id>",
                "--s3_secret_access_key=<your_key>"
            ])
 #Pipeline options to connect to S3 bucket and Extract csv data 

pickle_file_path = '../MongoDB_proj/postcode_dictionary.pkl' 
with open(pickle_file_path, 'rb') as f:
    dictionary = pickle.load(f)

def parse_csv_line(line):
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
    
postcodes=[]


def store_postcodes(element):
    # Store values in the global list
    postcodes.append(element[1]['Postcode'])
    return {"key":element[0],"value": element[1]}

def propkeyid_gen(p_key):
    
    if p_key in dictionary.keys():
        dictionary[p_key]+=1
    else:
        dictionary[p_key] = 1
    
    count_postc = dictionary[p_key]
    num = str(count_postc).zfill(4)
    
    property_key=p_key+'-'+num
    return property_key

class FormatInfo(beam.DoFn):
    def __init__(self, flag):
        self.flag = flag
    
    def process(self, data):

        if self.flag:
            property_id = propkeyid_gen(data[0].split(',')[-1])  # Generate property ID
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
            property_id = data[1]["prop_ids"][-1]
           
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
    # Pipeline 1
    
    with beam.Pipeline(options=options_aws) as p:
    # Read the input transaction data
        transactions = ( 
            p 
            | "Read Transaction Data" >> beam.io.ReadFromText("s3://bvkawsbucket/testdata.txt")
            | 'Parse CSV Lines' >> beam.Map(parse_csv_line)
            | beam.Filter(lambda x: x[12] == 'BIRMINGHAM')
            )

        transformed_transactions = (
            transactions
            | "Transform Transaction Data" >> beam.ParDo(ExtractInfo())
            | beam.Map(store_postcodes)
            | beam.Map(json.dumps)  # Convert data to JSON format
            | beam.io.WriteToText("output", file_name_suffix="")
            )
        
    # Pipeline 2
    filter_criteria = {"Postcode" : { "$in" : postcodes}}
    
    with beam.Pipeline() as pipeline2:
        # Read the data from the file as input to pipeline 2
        tuple_collection = (pipeline2
                             | beam.io.ReadFromText("output-00000-of-00001")
                            | beam.Map(lambda x : json.loads(x))
                            | beam.Map(lambda x : (x["key"],x["value"]))
        ) 

        prop_collection = (
                        pipeline2 
                        |ReadFromMongoDB(uri=connection_uri, db="propertyuk", coll="bham_prop", filter=filter_criteria, bucket_auto=True)
                        | beam.Map(lambda x: (','.join([x['PAON'], x['SAON'], x['Street'], x['Locality'], x['Postcode'].split(' ')[0]]),x['PropertyID']))
        )
      
        combined_data = (
                {'transactions': tuple_collection, 'prop_ids': prop_collection}
                | "Combine Data" >> beam.CoGroupByKey()
                |beam.Filter(lambda x :  len(x[1]["transactions"])>0)
        )
        
        prop_w_key = (combined_data  
            |beam.Filter(lambda x :  len(x[1]["prop_ids"])>0)
            | 'Formating prop_w_key Data' >> beam.ParDo(FormatInfo(False))
            | 'prop_w_key Data store' >> WriteToMongoDB(uri=connection_uri, db="propertyuk", coll="bham_tran")

        )

        prop_wo_key = ( 
            combined_data  
            |beam.Filter(lambda x :  len(x[1]["prop_ids"])==0)
            | 'Formating prop_wo_key Data' >> beam.ParDo(FormatInfo(True))
        )
        
        final_dump_tran= (
            prop_wo_key 
            | beam.Filter(lambda x: 'Tran_uid' in x)
            | 'tran_wo_key Data store' >> WriteToMongoDB(uri=connection_uri, db="propertyuk", coll="bham_tran")
        )

        final_dump_prop = (
            prop_wo_key 
            | beam.Filter(lambda x: 'Tran_uid' not in x)
            | 'prop_wo_key Data store' >> WriteToMongoDB(uri=connection_uri, db="propertyuk", coll="bham_prop")
        )
    
    postcode_output_path = 'postcode_dictionary_upd.pkl'

# Store the Postcode dictionary as a pickle file
    with open(postcode_output_path, 'wb') as f:
        pickle.dump(dictionary, f)   
    
    os.remove("output-00000-of-00001")
if __name__ == "__main__":
    main()   


