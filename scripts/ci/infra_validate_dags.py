from airflow.models import DagBag


def main() -> None:
    dag_bag = DagBag("dags/", include_examples=False)
    if dag_bag.import_errors:
        raise AssertionError(dag_bag.import_errors)
    print("DAGs valid")


if __name__ == "__main__":
    main()
