# Data Engineering Project: Spark and Airflow Integration

## Overview

This project integrates Apache Spark with Apache Airflow to create a robust ETL pipeline. The pipeline extracts data from a source, processes it using Spark, and loads it into a PostgreSQL database. The project is containerized using Docker, allowing for easy deployment and scalability.

## Project Structure

```
.
├── dags
│    └── ndt_oscar.py
│   
├── spark_apps
│       └── spark
│           └── dlk_to_staging_dim.py
│           └── dlk_to_staging_fact.py
│           └── stg_to_hist.py
│       └── sql
├── requirements
│   └── requirements.txt
├── conf
│   └── spark-defaults.conf
├── docker-compose.yml
├── Dockerfile.airflow
├── Dockerfile.spark
├── entrypoint-airflow.sh
├── entrypoint.sh
├── .env.spark
├── airflow.env
└── requirements_airflow.txt
```

## Technologies Used

- **Apache Airflow**: For orchestrating the ETL pipeline.
- **Apache Spark**: For data processing.
- **PostgreSQL**: For data storage.
- **Docker**: For containerization.
- **Python**: For scripting and data manipulation.

## Setup Instructions

1. **Clone the Repository**
   ```bash
   git clone <repository-url>
   cd <repository-directory>
   ```

2. **Build and Run Docker Containers**
   ```bash
   docker-compose up --build
   ```

3. **Access Airflow**
   - Open your browser and go to `http://localhost:8080`.

## DAGs

### `ndt_oscar.py`

This DAG orchestrates the ETL process, including:

- **File Sensor**: Monitors the presence of the input file.
- **Spark Jobs**: Submits Spark jobs for data processing.
- **Cleanup**: Cleans up temporary files after processing.

## Requirements

Install the required Python packages using:

```bash
pip install -r requirements/requirements.txt
```

## Configuration

- **Airflow Environment Variables**: Configured in `airflow.env`.
- **Spark Configuration**: Defined in `conf/spark-defaults.conf`.

## Contributing

Feel free to submit issues or pull requests for improvements or bug fixes.

## License

This project is licensed under the MIT License.
```
