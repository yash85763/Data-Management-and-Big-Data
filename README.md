# Data Management and Big Data
ETL-Data-Pipeline-Project-using-Big-Data-Technology

## Summary

This project focuses on creating a robust ETL (Extract, Transform, Load) data pipeline using Apache Spark on the Google Cloud Platform (GCP). The pipeline involves various steps, including Data Ingestion, Data Pre-processing, Data Transformation, Data Storage, and Data Transfer. By following industry standards and integrating with PyCharm IDE, we aim to automate the entire data pipeline.

## Introduction

ETL (Extract, Transform, Load) pipelines play a crucial role in moving and processing data between systems. In this project, we specifically focus on an ETL data pipeline for brain tumor detection. The pipeline involves extracting data from different sources, pre-processing it to cleanse and prepare the data, transforming it into a suitable format for analysis, and finally persisting it into storage spaces for further analytics.
Keywords

ETL, Data Pipeline, Data Ingestion, Data Pre-processing, Data Transformation, Data Persist (Data Storage), GCP (Google Cloud Platform), Amazon S3, Azure Blob Storage, PySpark, Apache Spark, HDFS (Hadoop Distributed File System), Hive, PostgreSQL, Linux OS.
Single Node Cluster Setup

The project begins with setting up a single node cluster on the Google Cloud Platform (GCP). A Linux OS, specifically Google Cloud Ubuntu 18.04 virtual machine, is used as the base. The choice of GCP is due to its easy VM instance setup and availability of necessary permissions and resources.

The following steps are performed for the cluster setup:

    * Python and Java Installation: Python and Java JDK are installed on the virtual machine as both Apache Spark and Hadoop require Java to function.

    * Secure Localhost Connection: Secure connection to localhost without a password is established, facilitating the starting of HDFS or YARN services without the need for entering passwords.

    * Hadoop and YARN Setup: Hadoop and YARN Manager are downloaded and configured on the virtual machine.

    * Hive Metastore Setup: Hive metastore is set up in local mode using PostgreSQL database to enable multiple Hive sessions and simultaneous usage by multiple users.

## Data Pipeline Steps

The data pipeline consists of the following key steps:

    * Data Ingestion: Raw data is collected from various sources, including different relational databases, file systems (e.g., .csv, .parquet, .orc, .json), repositories, and SaaS applications. The data is then brought into the HDFS via Linux commands on the GCP.

    * Data Pre-processing: Data is cleansed and prepared during this stage. Data cleansing operations are performed to ensure data quality.

    * Data Transformation: The core process of the data pipeline involves data standardization, de-duplication, sorting, validation, and other necessary transformations to make the data ready for analysis.

    * Data Storage: The transformed data is persisted into storage spaces, such as AWS S3 buckets, Azure Blob Storage, PostgreSQL database, and a local Linux path.

    * Data Transfer: The final files are transferred to Amazon S3 buckets, Azure Blob Storage, and a Linux path for further usage and analysis.

## Conclusion

This project demonstrates the implementation of an ETL data pipeline for brain tumor detection using Apache Spark on the Google Cloud Platform. The pipeline's steps are well-defined, integrated with industry standards, and automated, providing a solid foundation for scalable data processing and analysis.

## Contributing

If you wish to contribute to this project or have any feedback, please feel free to reach out to the project team.

## License

This project is distributed under the MIT license. See the [LICENSE.md](LICENSE) file for more details.
