# Supermarket Data Pipeline

## Overview

This project showcases a complete data pipeline designed for a supermarket dataset, integrating multiple sources of data, performing transformations, and automating workflows. The pipeline is built with modern data tools such as **Airbyte**, **DBT**, **PostgreSQL**, **AWS S3**, and **Airflow**.

## Workflow

1. **Data Integration (Airbyte)**:
    - Data is ingested from various sources into **PostgreSQL** using **Airbyte**. The sources could include cloud platforms, APIs, and third-party applications.
2. **Data Transformation (DBT)**:
    - After ingestion, **DBT** (Data Build Tool) is used to clean, transform, and prepare the data. This step ensures that only the necessary and accurate data is passed to the next stage.
3. **Data Upload (AWS S3)**:
    - The transformed data is offloaded to **AWS S3**, making it ready for further analysis or archiving.
4. **Automation (Airflow)**:
    - The entire pipeline is scheduled and automated using **Apache Airflow**. Airflow orchestrates the tasks, ensuring the pipeline runs seamlessly on a defined schedule.

## Tools & Technologies

- **Airbyte**: For data integration from multiple sources into PostgreSQL.
- **DBT**: For transforming and cleaning raw data.
- **PostgreSQL**: As the primary database for staging and storing ingested data.
- **AWS S3**: For storing processed data after transformation.
- **Airflow**: For scheduling and automating the entire pipeline.

## Getting Started

### Prerequisites

- Docker (for Airbyte, DBT, and Airflow)
- PostgreSQL
- AWS S3 account
- Python (for Airflow DAGs)

### Installation

1. **Airbyte Setup**:
    - Install and run Airbyte following the official documentation.
    - Connect your data sources and configure a destination to PostgreSQL.
2. **DBT Setup**:
    - Install DBT and set up the project.
    - Define models to clean and transform the ingested data.
3. **Airflow Setup**:
    - Install Airflow and configure the DAGs to run tasks for data integration, transformation, and offloading to S3.

### Usage

1. Use Airbyte to ingest data into PostgreSQL.
2. Run DBT models to clean and transform the data.
3. Use Airflow to schedule and automate the pipeline.

## Future Improvements

- Add more data sources and integrate with additional cloud platforms.
- Implement data quality checks using **Great Expectations** or **DBT tests**.
- Extend the pipeline to include real-time data streaming.
