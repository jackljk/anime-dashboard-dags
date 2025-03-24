import logging
from airflow.operators.email_operator import EmailOperator

EMAIL = "limjackailjk@gmail.com"  # Replace with your email address

def email_notification(context):
    """
    Callback function to send an email notification when a task fails.
    """
    task_instance = context['task_instance']
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    execution_date = context['execution_date']
    
    # Construct the email content
    subject = f"Task Failed: {task_id} in DAG: {dag_id}"
    body = f"Task {task_id} in DAG {dag_id} failed on {execution_date}."
    
    # Here you would integrate with your email service to send the email
    # For example, using smtplib or a service like SendGrid, etc.
    
    print(f"Sending email...\nSubject: {subject}\nBody: {body}")
    # Uncomment and implement actual email sending logic here
    email_operator = EmailOperator(
        task_id='send_email',
        to= EMAIL,
        subject=subject,
        html_content=body,
        dag=context['dag']
    )
    email_operator.execute(context=context) # This will send the email
