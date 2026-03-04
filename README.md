# Data Engineering & Retail Analytics Project – Walmart Chain

This is a data engineering project built as part of a university course on data analytics and business intelligence.
The goal is to build a complete pipeline starting from raw retail data, processing it through multiple layers,
storing it in a structured data warehouse, and finally presenting the results through an interactive web dashboard
with AI-powered analytics support.

The project covers three main areas: building an automated ETL pipeline, designing a Galaxy Schema data warehouse
with three star schemas, and creating a React-based dashboard connected to a local data service.

---

## Project Overview

The dataset used in this project comes from Walmart retail operations, covering three different dimensions:

- Customer purchase transactions from online channels
- Store performance data from 45 physical Walmart stores between 2010 and 2012
- Product catalog data collected through the Walmart API via SerpAPI

The pipeline processes these datasets through three layers (Raw, Clean, Golden), then loads the result
into a DuckDB data warehouse. The web application reads from this warehouse and provides three dashboards:
Revenue Trend Analysis, Customer Segmentation, and Store Sales Performance.

---

## Project Structure

```
DA/
    data/
        Raw/                    original source files
        Clean/                  cleaned files after silver pipeline
        Golden/
            standardized/       column-normalized files before dimension building
            dimensions/         DIM_PRODUCT, DIM_CUSTOMER, DIM_DATE, DIM_PAYMENT, DIM_CATEGORY
            facts/              FACT_SALES

    pipelines/
        silver/                 ETL pipeline: extract, transform, load to staging DuckDB
            extracting.py
            transforming.py
            loading.py
            run.py

        golden/                 Star schema pipeline: standardize, build dims, build facts
            standardize_columns.py
            build_dims.py
            build_facts.py
            validate_schema.py
            run_pipeline.py

        data_quality/           Data quality checks integrated into golden pipeline

    database/
        walmart_analytics.db    DuckDB warehouse with all DIM/FACT tables

    staging/
        staging.db              intermediate DuckDB staging database

    WalmartAPI/                 Separate API data collection module
        src/
            call_API.py         calls SerpAPI for Walmart product data
            clean_data.py       cleans the API response
            save_data.py        saves raw and processed data
            analyze_data.py     basic descriptive statistics
            eda_api.py          exploratory data analysis plots
        pipeline.py             runs all 5 steps in sequence
        Dockerfile              Docker setup for isolated API collection

    WEB/                        React web dashboard
        src/
            data/               JSON files exported from the warehouse
            services/
                dataAnalytics.js    query engine used by the AI chatbot
            components/
                Dashboards/     three main dashboard views
                ChatBot/        floating AI assistant (Groq API)
                Charts/         ChartAIHelper for per-chart AI popup
                Auth/           login page
                Welcome/        welcome screen with animations
                Reports/        PDF export component

    latex/                      documentation and chart generation for report
    diagrams/                   architecture and flow diagrams (PlantUML)
```

---

## Requirements

Python dependencies are listed in `requirement.txt`. The main ones used are:

- pandas, numpy – data manipulation
- duckdb – local data warehouse storage
- scikit-learn – used for KNN imputer in the transform step
- pyarrow – parquet support
- charset-normalizer – encoding detection for CSV files

For the WalmartAPI module, additional dependencies are in `WalmartAPI/requirements.txt`:

- serpapi – to call the Walmart product search endpoint
- matplotlib, seaborn – for EDA charts
- openpyxl – for Excel output

For the web dashboard, dependencies are managed through npm. The main libraries used are React, Recharts for
visualization, Tailwind CSS for styling, and the Groq SDK for AI features.

---

## How to Run

### Step 1 – Silver Pipeline (Raw to Clean)

This step reads all CSV files from `data/Raw`, applies transformations (handling missing values, standardizing
column names, removing duplicates), and loads the result into `staging/staging.db`. Cleaned CSVs are also
written to `data/Clean`.

```bash
cd pipelines/silver
python run.py
```

### Step 2 – Golden Pipeline (Clean to Star Schema)

This step reads the cleaned files, standardizes column names across sources, builds dimension tables
and the FACT_SALES table, then loads everything into `database/walmart_analytics.db`.

```bash
cd pipelines/golden
python run_pipeline.py
```

After running, you can validate that primary key and foreign key constraints hold:

```bash
python validate_schema.py
```

### Step 3 – WalmartAPI Data Collection (optional)

This module calls SerpAPI to collect Walmart product data. You need a SerpAPI key stored in a `.env` file
inside the WalmartAPI folder. The pipeline runs five steps: call API, save output, analyze, clean, and EDA.

```bash
cd WalmartAPI
cp .env.example .env   # then add your API_KEY
python pipeline.py
```

A Docker option is also available for isolated execution:

```bash
docker build -t walmart-api .
docker run --env-file .env walmart-api
```

### Step 4 – Web Dashboard

Make sure you have Node.js installed. Create a `.env` file in the WEB folder with your Groq API key:

```
VITE_GROQ_API_KEY=your_groq_api_key
```

Then install dependencies and run the development server:

```bash
cd WEB
npm install
npm run dev
```

The app will be available at `http://localhost:5173`.

---

## Data Warehouse Structure (Galaxy Schema)

The warehouse uses a Galaxy Schema design with three star schemas built on top of shared dimension tables.

Star Schema 1 – Retail Sales

    FACT_SALES
        sale_id (PK)
        date_key (FK -> DIM_DATE)
        customer_key (FK -> DIM_CUSTOMER)
        product_key (FK -> DIM_PRODUCT)
        payment_key (FK -> DIM_PAYMENT)
        category_key (FK -> DIM_CATEGORY)
        purchase_amount
        discount_applied
        rating
        repeat_customer

Star Schema 2 – Store Performance (aggregated weekly)

    FACT_STORE_PERFORMANCE
        store, date, weekly_sales
        temperature, fuel_price, cpi, unemployment
        holiday_flag, temp_category

Star Schema 3 – E-commerce Product Catalog

    FACT_ECOMMERCE_SALES
        product data collected from Walmart API
        pricing, ratings, brand, category information

---

## Web Dashboard Features

The dashboard has three main pages, each showing different aspects of the data:

- Revenue Trend Analysis shows monthly revenue trends, temperature impact on sales, holiday effects,
  weekday vs weekend patterns, and revenue breakdown by product category.

- Customer Segmentation shows repeat customer rates by age group, monthly customer counts,
  revenue contribution by age, and payment method preferences.

- Store Sales Performance shows sales by temperature category, unemployment vs weekly sales over time,
  fuel price trends, top performing stores, and CPI impact analysis.

Each chart has a small AI helper button in the corner. Clicking it opens a mini chat popup where you can
ask questions about that specific chart. There is also a floating chatbot (named Alyss) that can answer
questions about the full dataset using the Groq LLM API.

---

## Validation Output

Running `validate_schema.py` checks all primary keys and foreign keys in FACT_SALES:

```
DIM_PRODUCT: primary key is valid (17287 rows)
DIM_CUSTOMER: primary key is valid (50000 rows)
DIM_DATE: primary key is valid (365 rows)
DIM_PAYMENT: primary key is valid (4 rows)
DIM_CATEGORY: primary key is valid (4 rows)
FACT_SALES: primary key is valid (50000 rows)
FACT_SALES -> DIM_DATE: date_key valid
FACT_SALES -> DIM_CUSTOMER: customer_key valid
FACT_SALES -> DIM_PRODUCT: product_key valid
FACT_SALES -> DIM_PAYMENT: payment_key valid
FACT_SALES -> DIM_CATEGORY: category_key valid
Validation passed
```

---

## Notes

The Groq API key is stored in `WEB/.env` which is excluded from version control via `.gitignore`.
If you are running this project yourself, you need to create that file manually and add your own key.

The DuckDB databases (`staging.db` and `walmart_analytics.db`) are generated by running the pipelines.
They are not committed to the repository because they are binary files and can be reproduced by running
the pipeline steps above.

The web app is deployed on Vercel. Environment variables for Vercel need to be set in the project dashboard
under Settings > Environment Variables, using the same key name `VITE_GROQ_API_KEY`.

---

## Team

This project was developed as a data engineering capstone project.
The pipeline layer, schema design, and documentation were built in Python.
The web layer was built using React with Vite, Recharts, Tailwind CSS, and the Groq SDK.
