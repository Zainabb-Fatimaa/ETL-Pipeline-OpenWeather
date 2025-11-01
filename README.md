#  ETL Pipeline — OpenWeather & WeatherAPI Integration

##  Overview

A mini data engineering project that builds an **ETL pipeline** to collect real-time weather data for **Karachi, Lahore, and Islamabad** from **OpenWeather** and **WeatherAPI**, store it in multiple formats (**JSON, CSV, XML**), and load the cleaned data into a **Supabase (PostgreSQL)** cloud database.

##  Tech Highlights

* **Extract:** Weather data via OpenWeather & WeatherAPI
* **Transform:** Clean, merge, and standardize multi-format data
* **Load:** Push final datasets into **Supabase** using `supabase-py`
* **Stack:** Python · Pandas · Requests · Supabase · APIs · .env

##  Impact

* Demonstrates **end-to-end ETL automation** and **API data integration**
* Uses **dual data sources** for improved data accuracy
* Illustrates **cloud-based storage & transformation** in a practical context
* Strengthens understanding of **data pipelines**, **cloud databases**, and **ETL workflows**
