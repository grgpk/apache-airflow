from airflow.sdk import dag, task


@dag
def xcom_auto_dag():

    @task.python
    def first_task():
        print("Extracting data from source")
        fetched_data = {"data": [1, 2, 3, 4, 5]}
        return fetched_data

    @task.python
    def second_task(data: dict):
        print("Transforming data")
        transformed_data = [x * 2 for x in data["data"]]
        return transformed_data
        

    @task.python
    def third_task(data: list):
        print("This is the third task, Dag is complete")
        return sum(data)

    first = first_task()
    second = second_task(first)
    third = third_task(second)

xcom_auto_dag()