# Beam_ETL_UKProp

Repository for Apache Beam ETL project on HM Land Registry Price Paid Data.

The HM Land Registry Price Paid Data set is a valuable resource containing comprehensive information about property transactions in the United Kingdom. Analyzing this dataset can offer insights into real estate trends, market dynamics, and property valuation. However, working with big data sets like this requires an efficient and scalable ETL (Extract, Transform, Load) pipeline.

## [ETL 1](https://github.com/BVK23/Beam_ETL_UKProp/tree/main/ETL%201)

Pipeline to transform and load Historical [Price Paid data](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads#single-file) in MongoDB Database.

Full pipeline code available in the [Collab Notebook](https://colab.research.google.com/drive/164hv_14QChqeqKgc2arvwBYgOYWamaf6).

Link to Building a Scalable Big Data ETL Pipeline I [Medium Article](https://medium.com/@varunkrishna97/building-a-scalable-big-data-etl-pipeline-apache-beam-python-sdk-with-mongodb-and-s3-i-o-ab334edc9999).

## [ETL 2](https://github.com/BVK23/Beam_ETL_UKProp/tree/main/ETL%202)

ETL 2 represtation: ![GIF](https://github.com/BVK23/Beam_ETL_UKProp/blob/main/ETL%202/ETL2_gif.gif)

Pipeline to ingest new [monthly data](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads#april-2023-data-current-month)

Link to Building a Scalable Big Data ETL Pipeline II, [medium article](https://medium.com/@varunkrishna97/building-a-scalable-big-data-etl-pipeline-ii-apache-beam-python-sdk-with-mongodb-and-s3-i-o-59468a082e8b).

## Data Aggregation Logic

The key for aggregation is generated using a combination of the POAN (Primary Addressable Object Name), SOAN (Secondary Addressable Object Name), Street, Locality, and the first half of the Postcode. This logic is implemented to handle various scenarios and ensure the uniqueness of each property in the aggregation process.

The specific combination of attributes serves the following purposes:

1. Postcode Changes: Some properties may have experienced changes in the postcode over time due to administrative updates or renumbering, and in almost all cases, the last half is the one to undergo change. Therefore, to maintain consistency, we utilize other details of the property address.

2. Missing Property Information: In certain cases, one or more values such as PAON, SAON, Street, or Locality may be missing for a property. To ensure the uniqueness of each property, we combine all four values in the key.

3. Similar Property Information: There may be instances where multiple properties share the same PAON, SAON, Street, or any combination of these values. To differentiate between these similar properties, we include all four values along with the first characters of the postcode in the key.


## Property ID gen function

The propkeyid_gen() function is employed to generate the property key identifier based on the postcode. This function utilizes a dictionary named dict_post to keep track of the count of properties with the same postcode. The function checks if the postcode exists in the dictionary, updates the count accordingly, and generates a four-digit count, zero-padded if necessary. The postcode and count are then combined to form the property key identifier. We then store the dictionary in a pickle file to later use to generate Property ID  for new properties.

### Document Structure on MongoDB Atlas

![Image_of_Document_Struct](https://github.com/BVK23/Beam_ETL_UKProp/blob/main/ETL%201/MongoDB_coll_ilust.png)


---

Data Source : https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads 

Data Definition : https://www.gov.uk/guidance/about-the-price-paid-data#download-options

Contains HM Land Registry data © Crown copyright and database right 2021. This data is licensed under the Open Government Licence v3.0.
