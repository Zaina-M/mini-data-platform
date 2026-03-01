# CI/CD Overview

This document explains the Continuous Integration and Continuous Deployment practices for the Sales Data Platform.

---

## What is CI/CD for Data Platforms?

### Continuous Integration (CI)
Automatically testing code changes before they're merged. For data platforms, this includes:
- Code linting and style checks
- Unit tests for transformation logic
- Container build verification
- DAG syntax validation

### Continuous Deployment (CD)
Automatically deploying validated changes to environments. For data platforms:
- Deploying updated DAGs to Airflow
- Running database migrations
- Updating container images
- Validating data flow end-to-end

---

## CI Pipeline Components

### 1. Code Quality Checks

**Linting**
```yaml
- name: Lint Python
  run: |
    pip install flake8 black
    flake8 airflow/dags/ data-generator/
    black --check airflow/dags/ data-generator/
```

**Purpose:** Catch syntax errors and enforce consistent code style before review.

### 2. DAG Validation

**Airflow DAG Import Test**
```yaml
- name: Validate DAGs
  run: |
    pip install apache-airflow
    python -c "from airflow.models import DagBag; d = DagBag('airflow/dags/'); assert len(d.import_errors) == 0"
```

**Purpose:** Ensure DAGs can be parsed without errors. Broken DAGs won't load in Airflow.

### 3. Unit Tests

**Data Transformation Tests**
```yaml
- name: Run Unit Tests
  run: |
    pip install pytest pandas
    pytest tests/ -v
```

**Purpose:** Verify transformation logic produces expected outputs for known inputs.

Example test structure:
```python
def test_clean_data_removes_negative_quantities():
    input_data = [{'quantity': -5}, {'quantity': 10}]
    result = clean_data(input_data)
    assert all(r['quantity'] > 0 for r in result)
```

### 4. Container Build Verification

**Docker Build Test**
```yaml
- name: Build Containers
  run: |
    docker-compose build --no-cache
    docker-compose config
```

**Purpose:** Ensure Docker images build successfully and compose file is valid.

### 5. Security Scanning

**Container Vulnerability Scan**
```yaml
- name: Scan for Vulnerabilities
  uses: aquasecurity/trivy-action@master
  with:
    image-ref: 'mini-data-platform:latest'
```

**Purpose:** Detect known vulnerabilities in container dependencies.

---

## CD Pipeline Components

### 1. Test Environment Deployment

**Deploy to Test**
```yaml
- name: Deploy to Test
  run: |
    docker-compose -f docker-compose.test.yml up -d
    sleep 60  # Wait for services to initialize
```

**Purpose:** Spin up an isolated environment for integration testing.

### 2. Data Flow Validation

**End-to-End Test**
```yaml
- name: Test Data Flow
  run: |
    # Generate test data
    python data-generator/generate_sales.py --records 50 --upload
    
    # Trigger DAG
    docker exec airflow-webserver airflow dags trigger sales_data_pipeline
    
    # Wait for completion
    sleep 120
    
    # Verify data in PostgreSQL
    docker exec analytics-db psql -U analytics -c "SELECT COUNT(*) FROM sales"
```

**Purpose:** Verify the complete pipeline works with real data flow.

### 3. Database Migrations

**Schema Updates**
```yaml
- name: Run Migrations
  run: |
    for f in sql/migrations/*.sql; do
      docker exec analytics-db psql -U analytics -f "$f"
    done
```

**Purpose:** Apply database schema changes in a controlled manner.

### 4. Production Deployment

**Blue-Green Deployment**
```yaml
- name: Deploy to Production
  if: github.ref == 'refs/heads/main'
  run: |
    # Tag images with version
    docker tag mini-data-platform:latest registry/mini-data-platform:${{ github.sha }}
    docker push registry/mini-data-platform:${{ github.sha }}
    
    # Update deployment
    kubectl set image deployment/airflow airflow=registry/mini-data-platform:${{ github.sha }}
```

**Purpose:** Deploy validated changes to production with rollback capability.

---

## Provided GitHub Actions Workflow

The project includes `.github/workflows/ci.yml` with essential checks:

```yaml
name: CI Pipeline

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: pip install flake8 black
      - name: Run linters
        run: |
          flake8 airflow/dags/ data-generator/ --max-line-length=100
          black --check airflow/dags/ data-generator/

  validate-dags:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - name: Install Airflow
        run: pip install apache-airflow pandas minio psycopg2-binary
      - name: Validate DAGs
        run: |
          python -c "
          from airflow.models import DagBag
          dag_bag = DagBag('airflow/dags/', include_examples=False)
          assert len(dag_bag.import_errors) == 0, dag_bag.import_errors
          print(f'Validated {len(dag_bag.dags)} DAG(s)')
          "

  docker-build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Validate docker-compose
        run: docker-compose config -q
      - name: Build containers
        run: docker-compose build
```

---

## Data Quality in CI/CD

### Pre-merge Checks
- Schema validation tests
- Business rule validation
- Sample data processing

### Post-deployment Checks
- Row count verification
- Data freshness monitoring
- Aggregate consistency checks

### Example Data Quality Test
```python
def test_data_quality():
    """Verify processed data meets quality standards."""
    conn = get_postgres_connection()
    
    # Check for required fields
    result = conn.execute("""
        SELECT COUNT(*) FROM sales 
        WHERE order_id IS NULL OR total_amount IS NULL
    """)
    assert result.fetchone()[0] == 0, "Found NULL required fields"
    
    # Check for valid amounts
    result = conn.execute("""
        SELECT COUNT(*) FROM sales WHERE total_amount <= 0
    """)
    assert result.fetchone()[0] == 0, "Found invalid amounts"
```

---

## Environment Promotion Strategy

```
Feature Branch
     |
     v
  [CI Tests]
     |
     v
   Develop
     |
     v
  [Integration Tests]
     |
     v
   Staging
     |
     v
  [E2E Tests + Manual QA]
     |
     v
    Main
     |
     v
  Production
```

### Environment Purposes

| Environment | Purpose | Data |
|-------------|---------|------|
| Feature | Individual development | Synthetic |
| Develop | Integration testing | Synthetic |
| Staging | Pre-production validation | Anonymized production sample |
| Production | Live system | Real data |

---

## Rollback Procedures

### DAG Rollback
```bash
# Revert to previous DAG version
git revert HEAD
git push origin main
```

### Database Rollback
```bash
# Apply reverse migration
psql -f sql/migrations/rollback_001.sql
```

### Container Rollback
```bash
# Redeploy previous image
kubectl rollout undo deployment/airflow
```

---

## Monitoring CI/CD Health

### Key Metrics
- Build success rate
- Average build time
- Deployment frequency
- Change failure rate
- Mean time to recovery

### Alerting
- Notify on build failures
- Alert on deployment issues
- Page on production incidents

---

## Best Practices

1. **Run CI on every PR** - Catch issues before merge
2. **Keep builds fast** - Target under 10 minutes
3. **Test with realistic data** - Synthetic data that mimics production
4. **Automate everything** - Manual steps introduce errors
5. **Version your schema** - Track database changes in git
6. **Use feature flags** - Deploy code before enabling features
7. **Monitor deployments** - Know immediately when something breaks

---

## Future Enhancements

As the platform matures, consider adding:

1. **Data Contract Testing** - Validate producer/consumer agreements
2. **Performance Testing** - Benchmark pipeline throughput
3. **Chaos Engineering** - Test resilience to failures
4. **Canary Deployments** - Gradual rollout to detect issues
5. **Automated Rollback** - Self-healing on detected anomalies
