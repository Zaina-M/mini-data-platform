# Black Box Learning Guide

This document explains what the system does at a high level and provides a structured learning path for understanding the technologies involved.

## What Does This System Do? (The Black Box View)

From the outside, this system:

1. **Accepts** raw sales data in CSV format
2. **Stores** data in an object storage system
3. **Automatically processes** new files on a schedule
4. **Validates and cleans** the data
5. **Loads** clean data into an analytics database
6. **Visualizes** business metrics through dashboards

**Input:** CSV files with sales transactions
**Output:** Interactive dashboards showing sales performance

You don't need to understand every internal component to use this system. However, to maintain and extend it, you'll need to learn the technologies at different depths.

---

## Learning Priorities

### Learn Now (Mandatory for This Project)

These concepts are essential to understand before you can work with or modify this project.

#### Docker Basics
- **What to know:** Containers are isolated environments that package applications with their dependencies
- **Why it matters:** Every component in this platform runs in a container
- **Key concepts:** Images, containers, volumes, exposed ports
- **Minimum requirement:** Understand `docker-compose up`, `docker-compose down`, and how to view logs

#### Docker Compose
- **What to know:** Docker Compose defines multi-container applications
- **Why it matters:** This project uses 7+ interconnected services
- **Key concepts:** Services, networks, volumes, environment variables, depends_on
- **Minimum requirement:** Read and understand docker-compose.yml structure

#### Airflow DAG Fundamentals
- **What to know:** A DAG (Directed Acyclic Graph) defines task dependencies
- **Why it matters:** The entire data pipeline is defined as a DAG
- **Key concepts:** Tasks, operators, dependencies, scheduling
- **Minimum requirement:** Understand how tasks connect with `>>` operator

#### SQL Basics
- **What to know:** SQL is used to define schemas and query data
- **Why it matters:** Analytics database uses SQL
- **Key concepts:** CREATE TABLE, INSERT, SELECT, indexes
- **Minimum requirement:** Read and understand the init.sql schema

#### CSV File Structure
- **What to know:** CSV files are comma-separated text files
- **Why it matters:** Raw data input format
- **Key concepts:** Headers, delimiters, data types
- **Minimum requirement:** Understand expected column structure

---

### Learn Later (Good to Know After Submission)

These concepts will help you extend the project and troubleshoot issues, but aren't blocking for initial completion.

#### Docker Networking
- **What to know:** Containers communicate over virtual networks
- **Why it matters:** Services find each other by container name
- **Key concepts:** Bridge networks, DNS resolution, port mapping
- **When to learn:** When debugging connectivity issues between services

#### Object Storage Concepts
- **What to know:** Object storage stores files as objects with metadata
- **Why it matters:** MinIO stores raw and archived data
- **Key concepts:** Buckets, objects, S3-compatible APIs
- **When to learn:** When you need to modify storage patterns

#### Data Validation Patterns
- **What to know:** Data validation ensures data quality before processing
- **Why it matters:** Bad data can break pipelines and corrupt analytics
- **Key concepts:** Schema validation, business rules, error handling
- **When to learn:** When adding new data sources or rules

#### Airflow XCom
- **What to know:** XCom allows tasks to share data
- **Why it matters:** Pipeline tasks pass file lists and data between steps
- **Key concepts:** xcom_push, xcom_pull, task instance context
- **When to learn:** When modifying task communication patterns

#### Database Indexing
- **What to know:** Indexes speed up query performance
- **Why it matters:** Analytics queries need fast response times
- **Key concepts:** B-tree indexes, composite indexes, query planning
- **When to learn:** When optimizing dashboard performance

---

### Advanced (Industry-Level, Not Required Now)

These are production-grade concerns that real data platforms deal with. You don't need these for learning, but they're important career knowledge.

#### Batch vs Streaming Processing
- **What it is:** Batch processes data in chunks; streaming processes continuously
- **This project:** Uses batch processing (scheduled DAG runs)
- **Real-world:** Many systems use both (Lambda architecture)
- **When relevant:** When data freshness requirements change

#### CI/CD for Data Platforms
- **What it is:** Automated testing and deployment of data pipelines
- **This project:** Basic CI/CD workflow provided
- **Real-world:** Includes data quality tests, schema migration, canary deployments
- **When relevant:** When managing production data platforms

#### Infrastructure as Code
- **What it is:** Defining infrastructure in version-controlled files
- **This project:** Docker Compose is a simple form of IaC
- **Real-world:** Terraform, Pulumi, AWS CloudFormation
- **When relevant:** When deploying to cloud environments

#### Data Lineage and Observability
- **What it is:** Tracking data origin, transformations, and quality over time
- **This project:** Basic logging only
- **Real-world:** Tools like Great Expectations, dbt, Monte Carlo
- **When relevant:** When data trust and debugging matter

#### Secrets Management
- **What it is:** Secure storage and rotation of credentials
- **This project:** Hardcoded credentials (development only)
- **Real-world:** HashiCorp Vault, AWS Secrets Manager
- **When relevant:** Before any production deployment

#### Horizontal Scaling
- **What it is:** Adding more workers to handle increased load
- **This project:** Single instance of each service
- **Real-world:** Kubernetes, Celery executors, distributed databases
- **When relevant:** When processing large data volumes

---

## Learning Path Summary

| Phase | Topics | Time Investment |
|-------|--------|-----------------|
| **Now** | Docker basics, Compose, DAG fundamentals, SQL basics | 4-8 hours |
| **Later** | Networking, object storage, validation, XCom | 8-16 hours |
| **Advanced** | CI/CD, IaC, observability, scaling | Ongoing career learning |

---

## Recommended Resources

### Docker
- Docker official documentation: "Get Started" guide
- Docker Compose documentation: "Compose file reference"

### Apache Airflow
- Airflow official documentation: "Concepts" section
- Airflow tutorials: "Writing your first DAG"

### PostgreSQL
- PostgreSQL documentation: "Tutorial" section
- SQL basics: Any SQL fundamentals course

### General Data Engineering
- "Fundamentals of Data Engineering" by Joe Reis and Matt Housley
- Data Engineering Zoomcamp (free online course)

---

## Questions to Test Your Understanding

Before modifying this project, you should be able to answer:

1. What happens when you run `docker-compose up`?
2. How do containers communicate with each other?
3. What triggers the Airflow DAG to run?
4. Where does raw data go before processing?
5. What happens to files after successful processing?
6. How does Metabase access the analytics data?

If you can't answer these, revisit the "Learn Now" topics above.
