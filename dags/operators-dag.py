from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator


@dag
def operators_dag():

    @task.python
    def first_task():
        print("This is the first task")

    @task.python
    def second_task():
        print("This is the second task")

    @task.python
    def third_task():
        print("This is the third task, Dag is complete")

    @task.bash 
    def fourth_task():
        return "echo 'This is the fourth and bash task, Dag is complete'"
    
    bash_task_oldschool = BashOperator(
        task_id='bash_task_oldschool',
        bash_command="echo 'This is the old school bash task, Dag is complete'"
    )

    first_task() >> second_task() >> third_task() >> fourth_task() >> bash_task_oldschool

operators_dag()