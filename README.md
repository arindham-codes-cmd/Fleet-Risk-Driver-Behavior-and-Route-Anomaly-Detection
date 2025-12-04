**Originally developed by Arindham Krishna**  
GitHub: [arindham-codes-cmd](https://github.com/arindham-codes-cmd)

# Fleet-Risk-Driver-Behavior-and-Route-Anomaly-Detection
This project analyzes large scale GPS telematics data to evaluate driver behavior, compute risk scores, and detect route anomalies across real world fleet trips. The end-to-end pipeline integrates Snowflake SQL, Python based machine learning, and Power BI visualization to provide actionable fleet insights.

# Project Overview
- This dataset consists of 120,000+ trip records, each containing 26 behavioral, environmental, and route specific attributes.
- We built a structured Snowflake warehouse, performed multi layered data transformations, engineered trip-level and driver-level metrics, generated SQL based risk scores for drivers, and trained an ML model to predict risky trips.
- The results were visualized through a Power BI dashboard highlighting driver risk ranking, anomaly distribution, and ML predictions.

This project demonstrates a practical, production-style workflow that mirrors how modern fleet safety and telematics systems operate.
# Business Problem
**“Identify risky driver behavior and detect route anomalies in order to improve fleet safety, reduce operational incidents, and support proactive intervention.”**
The Fleet companies rely on consistent and safe driving. Micro-behaviors and hard braking, unstable steering, route deviations, geofencing violations all scale into safety risks and operational inefficiencies. With timestamped GPS telemetry and behavioral metrics, we can quantify risk, rank drivers, highlight anomalies, and predict unsafe trips before they escalate.

# Project Blueprint
## Phase A–I: Snowflake Ingestion, Cleaning & Transformations

We created a multi-schema Snowflake warehouse:
RAW → CLEAN → ANALYTICS

Key transformations included:
- Cleaning timestamps, coordinates, and categorical values
- Converting trip metrics from raw format
- Aggregating trip summary metrics
- Daily driver rollups
- Creating a structured analytics layer used for ML and BI

  
