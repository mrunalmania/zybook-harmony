# Zybook-Harmony
Capture live data from the ASU student's zybook feed. 

**Description:**
Zybook-Harmony is an ETL automation project aimed at capturing live data from the ASU student's Zybook feed. This project utilizes various AWS services such as Amazon S3, AWS Glue, Kinesis Data Streams, and Lambda functions to seamlessly transfer data, perform real-time data cleaning, and optimize data flow between different platforms.

# Architecture
![AWS-Zybook-Arch](https://github.com/mrunalmania/zybook-harmony/assets/92011740/774aaa7f-b364-49d0-8050-f807e6cb382c)

**Architecture Overview:**
- **DataStreams:** Data is captured from the Zybook feed and transferred to the ETL script.
- **AWS Services Used:**
  - **Amazon S3:** Used for storing data and as output bucket for Glue.
  - **AWS Glue:** Used for ETL (Extract, Transform, Load) operations.
  - **Kinesis Data Streams:** Used for real-time data streaming.
  - **AWS Lambda:** Used for executing Python functions for data cleaning and processing.
- **Integration Points:**
  - Zybook (3rd party vendor)
  - Google Drive (LTH-HUB)

**Key Features:**
- Led ETL development and AWS pipeline implementation for seamless data transfer from Zybook to Kinesis, reducing data update time by 95%.
- Conducted Exploratory Data Analysis (EDA) and designed AWS Lambda functions in Python for real-time data cleaning, resulting in high-quality datasets.
- Optimized data flow between Airtable and Redshift for efficient data processing and analysis.

**Repository Contents:**
- **userCSV.py:** Python script for extracting user data from Zybook.
- **integrate_drive.py:** Script for integrating data with Google Drive (LTH-HUB).
- **eti-filter.py:** Lambda function triggered by data updates, performs data filtering.
- **mapping.py:** Script for mapping data for integration with Airtable.

**Instructions for Deployment:**
1. Clone the repository from GitHub.
2. Set up AWS credentials and configure AWS services (S3, Glue, Kinesis, Lambda) as per the provided architecture.
3. Ensure proper configuration of scripts for data extraction, transformation, and loading.
4. Test the pipeline with sample data to verify functionality.
5. Deploy the pipeline for live data capture from Zybook feed.
