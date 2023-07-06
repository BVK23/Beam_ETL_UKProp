# ETL 2

## [sequential_pipeline.py](https://github.com/BVK23/Beam_ETL_UKProp/blob/main/ETL%202/sequential_pipeline.py)

This file contains the local test implementation of an ETL process for ingesting new monthly data of HM Land Registry Price Paid Data for the Birmingham District.

Most of the functions are similar to ETL 1 due to nature of the data and our processing logic. Some have been changed slightly to accomodate our implementation of the pipeline.

The new month data is slightly different from the historical pp-complete.csv data 

{F87E72FA-026A-176C-E053-6B04A8C0D2BE},160000,2023-02-09 00:00,"B1 2LJ","F","N","L","CUTLASS COURT, 30","APARTMENT 121","GRANVILLE STREET","","BIRMINGHAM","BIRMINGHAM","WEST MIDLANDS","B","A" 

Historical data : 

"{DBA933FA-5665-669D-E053-6B04A8C0AD56}","232999","2022-01-27 00:00","HU15 1FT","F","N","L","FAIRWAY VIEW","APARTMENT 27","ELLOUGHTON ROAD","","BROUGH","EAST RIDING OF YORKSHIRE","EAST RIDING OF YORKSHIRE","A","A"

So the parse_csv_line function is modified to accommodate that.

Initial solution Representation:
![GIF](https://github.com/BVK23/Beam_ETL_UKProp/blob/main/ETL%202/ETL2_gif.gif)


### [ETL 2 for Dataflow](https://github.com/BVK23/Beam_ETL_UKProp/blob/main/ETL%202/ETL_2_Dataflow_job.py)

Firestore is used to access and store the Property ID generator instead of a dictionary and pickle file.

Use of Pipeline options to run Dataflow job

setup() method used inside ParDo transform to initiate the Firestore client

Storing Postcodes in Google Storage Bucket instead of a list for filtering our ReadFromMongoDB

ETL2 solution for Dataflow Representation:
![GIF](https://github.com/BVK23/Beam_ETL_UKProp/blob/main/ETL%202/ETL2.gif)


