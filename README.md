# Beam_ETL_UKProp
Repo for Apache Beam ETL project on HM Land Registry Price Paid Data


Full pipeline code at [Collab_Notebook](https://colab.research.google.com/drive/164hv_14QChqeqKgc2arvwBYgOYWamaf6)

Link to [Medium Article](https://colab.research.google.com/drive/164hv_14QChqeqKgc2arvwBYgOYWamaf6](https://medium.com/@varunkrishna97/building-a-scalable-big-data-etl-pipeline-apache-beam-python-sdk-with-mongodb-and-s3-i-o-ab334edc9999)


# Logic behind Key for aggregating 
We generate the key by using the POAN, SOAN, Street, Locallity and the first half of the Postcode. The reason being 
