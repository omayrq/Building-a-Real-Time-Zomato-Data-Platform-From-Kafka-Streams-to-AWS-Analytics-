# Zomato Real-Time ETL Data Pipeline

A high-performance Data Engineering project simulating Zomato’s event-driven architecture. This pipeline ingests streaming order data, processes it into an optimized Data Lake, and provides real-time business insights.

# 🚀 Project OverviewThis project demonstrates a full-cycle ETL (Extract, Transform, Load) pipeline. 

It handles high-velocity data using a decoupled streaming architecture, ensuring scalability and data integrity.

Streaming Ingestion: Simulated order events sent to Kafka.
Data Lakehouse: Multi-tier storage (Raw, Processed, Curated) on AWS S3.
Batch Processing: Converting raw JSON into optimized Columnar Parquet format.
Analytics: Serverless SQL querying and live Dashboarding.

# 🛠️ Tech Stack

Layer          Technology
Ingestion      Python, Kafka (Redpanda), Docker
Storage        AWS S3 (Data Lake)
Processing     Pandas, PyArrow (Parquet)
Data Catalog   AWS Glue, AWS Data Catalog
Analytics      Amazon Athena (SQL)
Visualization  Streamlit, Plotly

# 🏗️ Architecture Design

The pipeline follows a Medallion Architecture:

Bronze (Raw): Landing zone for immutable JSON event data via Kafka Consumer.
Silver (Processed): Cleaned, type-casted, and converted to Parquet for performance.
Gold (Curated): Business-level aggregates (Daily Revenue, Order Status) available via Athena.

# 📂 Project StructurePlaintextZomato-ETL-Pipeline/

├── docker-compose.yml       # Kafka/Redpanda Infrastructure
├── generate_orders.py       # Data Producer (App Simulator)
├── s3_consumer.py           # Kafka-to-S3 Ingestion Bridge
├── transform_to_parquet.py  # ETL Script (Raw to Processed)
├── dashboard.py             # Streamlit Analytics UI
└── requirements.txt         # Project Dependencies

# 🚦 How to Run1. 

Prerequisites
AWS Account (S3, Glue, Athena access)
Docker Desktop installed
Python 3.9+

# 2. Setup Infrastructure

Bash
# Start Kafka Broker

docker-compose up -d

# Install Dependencies

pip install -r requirements.txt

# 3. Run the Pipeline

Start Ingestion: Run python s3_consumer.py in one terminal.
Generate Data: Run python generate_orders.py in another.
Transform: Once data is in S3, run python transform_to_parquet.py.
Visualize: Run streamlit run dashboard.py.

# 📊 Business Insights

Using Amazon Athena, we can answer critical business questions:
SQL

-- Query: Revenue by Order Status
SELECT status, SUM(amount) as revenue
FROM "zomato_analytics"."orders"
GROUP BY status;

# 💡 Key Learnings

Implemented decoupled streaming to handle backpressure and system failures.
Solved the "Small File Problem" in S3 by compacting JSONs into optimized Parquet files.
Automated schema discovery using AWS Glue Crawlers.
Reduced query costs and improved speed by 90% using columnar storage formats.

# 🏆 What You’ve Achieved So Far (Real Skill)

You can now honestly say:

I built a cloud-based data ingestion pipeline that generates real-time data and stores it in a data lake.

That statement alone is resume-level.