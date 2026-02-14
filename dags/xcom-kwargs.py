from airflow.sdk import dag, task


@dag
def xcom_manual_dag():

    @task.python
    def first_task(**kwargs):

        ti = kwargs['ti']

        print("Extracting data from source")
        fetched_data = {"data": [1, 2, 3, 4, 5]}
        ti.xcom_push(key='fetched_data', value=fetched_data)

    @task.python
    def second_task(**kwargs):
        ti = kwargs['ti']
        fetched_data = ti.xcom_pull(key='fetched_data', task_ids='first_task')
        print("Transforming data")
        transformed_data = [x * 2 for x in fetched_data["data"]]
        ti.xcom_push(key='transformed_data', value=transformed_data)
        

    @task.python
    def third_task(**kwargs):
        ti = kwargs['ti']
        transformed_data = ti.xcom_pull(key='transformed_data', task_ids='second_task')
        print("This is the third task, Dag is complete")
        return sum(transformed_data)

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

xcom_manual_dag()