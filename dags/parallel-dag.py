from airflow.sdk import dag, task


@dag
def parallel_dag():

    @task.python
    def first_task(**kwargs):
        ti = kwargs['ti']
        print("Extracting data from source")
        extracted_data = {"api_data": [1, 2, 3], "db_data": [4, 5, 6], "file_data": [7, 8, 9]}
        ti.xcom_push(key='extracted_data', value=extracted_data)

    @task.python
    def api_data_task(**kwargs):
        ti = kwargs['ti']
        extracted_data = ti.xcom_pull(key='extracted_data', task_ids='first_task')
        api_data = extracted_data['api_data']
        print("Processing API data:", api_data)
        transformed_api_data = [x * 2 for x in api_data]
        ti.xcom_push(key='api_data', value=transformed_api_data)

    @task.python
    def db_data_task(**kwargs):
        ti = kwargs['ti']
        extracted_data = ti.xcom_pull(key='extracted_data', task_ids='first_task')
        db_data = extracted_data['db_data']
        print("Processing DB data:", db_data)
        transformed_db_data = [x * 2 for x in db_data]
        ti.xcom_push(key='db_data', value=transformed_db_data)

    @task.python
    def file_data_task(**kwargs):
        ti = kwargs['ti']
        extracted_data = ti.xcom_pull(key='extracted_data', task_ids='first_task')
        file_data = extracted_data['file_data']
        print("Processing file data:", file_data)
        transformed_file_data = [x * 2 for x in file_data]
        ti.xcom_push(key='file_data', value=transformed_file_data)

    @task.bash
    def final_task(**kwargs):
        ti = kwargs['ti']
        api_data = ti.xcom_pull(key='api_data', task_ids='api_data_task')
        db_data = ti.xcom_pull(key='db_data', task_ids='db_data_task')
        file_data = ti.xcom_pull(key='file_data', task_ids='file_data_task')
        print("Final Task: Combining all transformed data")
        return f'echo "API Data: {api_data}, DB Data: {db_data}, File Data: {file_data}"'
    
    first = first_task()
    api_task = api_data_task()
    db_task = db_data_task()
    file_task = file_data_task()
    final = final_task()

    first >> [api_task, db_task, file_task] >> final
    
parallel_dag()