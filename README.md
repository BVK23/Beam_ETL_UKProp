# Beam_ETL_UKProp

Repository for Apache Beam ETL project on HM Land Registry Price Paid Data.

Full pipeline code available in the [Collab Notebook](https://colab.research.google.com/drive/164hv_14QChqeqKgc2arvwBYgOYWamaf6).

Link to the related [Medium Article](https://medium.com/@varunkrishna97/building-a-scalable-big-data-etl-pipeline-apache-beam-python-sdk-with-mongodb-and-s3-i-o-ab334edc9999).

## The Key Aggregation Logic

The key for aggregation is generated using a combination of the POAN (Primary Addressable Object Name), SOAN (Secondary Addressable Object Name), Street, Locality, and the first half of the Postcode. This logic is implemented to handle various scenarios and ensure the uniqueness of each property in the aggregation process.

The specific combination of attributes serves the following purposes:

1. Postcode Changes: Some properties may have experienced changes in the postcode over time due to administrative updates or renumbering, and in almost all cases, the last half is the one to undergo change. Therefore, to maintain consistency, we utilize other details of the property address.

2. Missing Property Information: In certain cases, one or more values such as PAON, SAON, Street, or Locality may be missing for a property. To ensure the uniqueness of each property, we combine all four values in the key.

3. Similar Property Information: There may be instances where multiple properties share the same PAON, SAON, Street, or any combination of these values. To differentiate between these similar properties, we include all four values along with the first characters of the postcode in the key.

---
