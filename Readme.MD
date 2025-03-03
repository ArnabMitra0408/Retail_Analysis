# Retail Analysis Project Using AWS and Tableau

## Overview
This project focuses on analyzing retail transaction data using AWS cloud services and Tableau. The workflow involves creating an ETL pipeline to process raw data, store it in a PostgreSQL database on Amazon RDS, and visualize the insights in Tableau.

## Project Structure
```
Retail_Analysis_AWS/
│-- raw_data/                # Raw CSV files stored in S3
│-- scripts/
│   │-- retail_customer.py      # Glue job script for customer data
│   │-- retail_feedback.py      # Glue job script for feedback data
│   │-- retail_gender.py        # Glue job script for gender data
│   │-- retail_income.py        # Glue job script for income data
│   │-- retail_location.py      # Glue job script for location data
│   │-- retail_order_status.py  # Glue job script for order status data
│   │-- retail_payment_methods.py # Glue job script for payment methods
│   │-- retail_product.py       # Glue job script for product data
│   │-- retail_sales_full_load.py # Glue job script for sales data
│   │-- retail_shipping_method.py # Glue job script for shipping method data
│-- lambda/
│   │-- trigger_pipeline.py    # AWS Lambda function to trigger Glue jobs
│-- sql_views/
│   │-- retail_data_view.sql   # SQL View for Tableau data source
│-- tableau/
│   │-- dashboard.twb          # Tableau Dashboard file
│-- README.md                 # Project documentation
```

## Dataset
The dataset consists of 302,010 records with 28 columns, including transaction details, customer demographics, product information, payment methods, and order status. The raw data is stored in an S3 bucket within a `raw_data` folder, tracked using Data Version Control (DVC) before processing.

## Workflow
1. **Data Ingestion**: The CSV data is stored in an Amazon S3 bucket.
2. **Database Setup**: A PostgreSQL database on Amazon RDS is used as the main data warehouse, with structured fact and dimension tables.
3. **ETL Pipeline with AWS Glue**: Multiple AWS Glue jobs transform and load data into the database, ensuring data integrity and relational structure.
4. **Pipeline Automation**: An AWS Lambda function triggers the Glue jobs whenever a new file is uploaded to S3, automating the ETL process.
5. **Monitoring**: AWS CloudWatch logs and tracks pipeline execution and alerts failures.
6. **SQL View for Reporting**: A SQL view aggregates data from multiple tables, simplifying analysis and serving as the primary data source for Tableau.
7. **Data Visualization in Tableau**: The cleaned and processed data is visualized in Tableau dashboards to provide insights into retail trends, customer behavior, and sales patterns.

## AWS Services Used
- **Amazon S3**: Stores raw data files.
- **AWS Lambda**: Automates ETL execution upon file upload.
- **AWS Glue**: Processes and loads data into RDS.
- **Amazon RDS (PostgreSQL)**: Serves as the central data warehouse.
- **AWS CloudWatch**: Monitors pipeline execution and logs events.

## Data Insights
The processed data allows for detailed retail analytics, including:
- Customer demographics and purchasing behavior.
- Sales trends by product category, brand, and region.
- Order statuses, payment methods, and shipping preferences.
- Customer feedback and satisfaction ratings.

## Next Steps
- Enhance the ETL pipeline for real-time data processing.
- Implement machine learning models for demand forecasting and customer segmentation.
- Expand Tableau dashboards with advanced KPIs and interactive visualizations.

This project demonstrates a scalable, cloud-based retail analytics solution integrating AWS services and Tableau to extract meaningful business insights from large-scale transaction data.
