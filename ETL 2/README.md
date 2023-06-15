# ETL 1

## [uk_property_data_etl.py](https://github.com/BVK23/Beam_ETL_UKProp/blob/main/ETL%202/sequential_pipeline.py)
This file contains the local test implementation of an ETL process for storing historical data of HM Land Registry Price Paid Data for the Birmingham District.

The updates mentioned are derived from the first version, i.e., the [colab notebook](https://github.com/BVK23/Beam_ETL_UKProp/blob/main/Apache_Beam_ETL_Pipeline_UK_Property_Data.ipynb).

Also, all code is modified to run locally for test purposes. The `WriteToMongoDB` transform and reading data from a CSV stored on S3 are not included. Please check the colab notebook for the usage of those transforms.

### Update 1
1. Property ID dictionary storage using [Pickle]().
2. Pipeline runner options.
3. Refactor code: Use of Composite Transforms.

<!--  ### Update 2 -->


