# E-Commerce Seller Recommendation System

## Overview

This project implements an end-to-end Data Engineering pipeline that transforms raw, unstructured e-commerce data into actionable seller-level product recommendations.

The system analyzes company sales data, competitor sales data, and seller catalog information to generate the **Top 10 product recommendations for each seller**, while excluding products that already exist in the seller's catalog.

The primary objective is to help sellers identify profitable products and categories that they are not currently selling, enabling data-driven business expansion and increased revenue opportunities.

---

## Business Problem

In large e-commerce marketplaces, sellers often miss opportunities to expand into high-performing product categories.

While competitor sellers may generate significant revenue from certain products, many sellers remain unaware of these opportunities.

This system identifies such gaps by:

* Analyzing sales performance across the marketplace
* Evaluating product popularity and business potential
* Comparing seller catalogs against available opportunities
* Recommending products not currently sold by the seller

---

## Project Objectives

* Ingest raw e-commerce datasets from multiple sources
* Perform data validation and quality checks
* Standardize and clean inconsistent records
* Build a layered data lake architecture
* Store processed data using scalable incremental storage
* Generate seller-specific Top 10 product recommendations
* Exclude products already present in the seller catalog
* Produce business-ready recommendation datasets

---

## Data Sources

The pipeline processes data from the following sources:

### Company Sales Data

Contains transaction-level sales information generated within the organization.

### Competitor Sales Data

Contains market and competitor sales information used to identify profitable products and categories.

### Seller Catalog Data

Contains products currently offered by each seller.

---

## Data Challenges

The raw datasets may contain:

* Missing values
* Invalid records
* Schema inconsistencies
* Duplicate records
* Incorrect data types
* Incomplete product information

The pipeline addresses these issues through automated Data Quality (DQ) validations.

---

## Architecture

The project follows a layered Data Lake Architecture.
## Architecture

![Architecture Diagram](architecture.png)

Stores raw ingested files exactly as received from source systems.

### Bronze Layer

Performs initial ingestion and standardization while preserving source-level details.

### Silver Layer

Applies data quality validations, cleansing rules, schema standardization, and business transformations.

### Gold Layer (Apache Hudi)

Stores final recommendation datasets optimized for analytics and downstream consumption.

---

## Data Quality Framework

The pipeline incorporates multiple Data Quality checks to improve reliability and trustworthiness.

### Schema Validation

* Column existence checks
* Data type validation
* Structural consistency checks

### Null Validation

* Detection of mandatory field violations
* Identification of incomplete records

### Duplicate Detection

* Duplicate transaction identification
* Duplicate product detection

### Business Rule Validation

* Invalid product information checks
* Invalid seller mappings
* Incorrect category assignments

### Data Standardization

* Column normalization
* Data type correction
* Consistent formatting across datasets

---

## Recommendation Logic

The recommendation engine generates seller-specific recommendations by:

1. Analyzing company sales performance
2. Analyzing competitor sales performance
3. Identifying high-performing products and categories
4. Comparing opportunities against the seller's existing catalog
5. Excluding already-listed products
6. Ranking candidate products
7. Generating Top 10 recommendations per seller

---

## Technology Stack

* Python
* PySpark
* Apache Spark
* Apache Hudi
* YAML Configuration
* Data Lake Architecture
* ETL Pipelines
* Data Quality Framework
* Git & GitHub

---

## Project Structure

```text
project/
│
├── configs/
│   └── ecomm_prod.yml
│
├── scripts/
│   ├── etl_company_sales_spark.py
│   ├── etl_competitor_sales_spark.py
│   ├── etl_seller_catalog_spark.py
│   └── recommendation_engine.py
│
├── src/
│   ├── etl_company_sales.py
│   ├── etl_competitor_sales.py
│   ├── etl_seller_catalog.py
│   └── recommendation_engine.py
│
└── output/
    └── gold_hudi_tables/
```

---

## Key Features

* End-to-End Data Engineering Pipeline
* Multi-Source Data Integration
* Config-Driven Architecture
* Data Quality Validation Framework
* Layered Data Lake Design
* Incremental Data Processing
* Apache Hudi Storage Layer
* Seller-Level Recommendation Engine
* Scalable Spark-Based Processing

---

## Future Enhancements

* Machine Learning-Based Recommendation Engine
* Real-Time Recommendation Pipeline
* Airflow Workflow Orchestration
* Data Quality Monitoring Dashboard
* Recommendation Performance Tracking
* Seller Feedback Loop Integration

---

## Outcome

The final output consists of seller-level Top 10 product recommendations generated from validated and transformed e-commerce datasets.

These recommendations help sellers identify new revenue opportunities while enabling the platform to improve product coverage and marketplace growth.
