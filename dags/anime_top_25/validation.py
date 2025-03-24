import logging

def validate_transformed_data(ti, **kwargs):
    """
    Validate the extracted data to ensure it meets expected criteria.
    If validation fails, log the error and re-raise the exception to trigger notifications.
    """
    try:
        data = ti.xcom_pull(task_ids='extract_task_id')
        if data is None:
            raise ValueError("No data found from extract task")
        
        # Check expected number of entries
        if len(data) != 25:
            raise ValueError(f"Expected 25 entries in the data but got {len(data)}")
        
        ranks = set()
        keys = ['mal_id', 'title', 'url', 'image_url', 'title_english', 'title_japanese',
                'licensors', 'studios', 'genres', 'statistics']
        # Validate each data item
        for item in data:
            if not all(key in item for key in keys):
                raise ValueError(f"Missing keys in item: {item}")
            
            # Check for unique ranks
            rank = item['statistics']['rank']
            if rank in ranks:
                raise ValueError(f"Duplicate rank found: {rank}")
            ranks.add(rank)
            
            # Ensure all statistics have non-empty values
            for stat_key, stat in item['statistics'].items():
                if not stat:
                    raise ValueError(f"Statistic '{stat_key}' is empty in item: {item}")
        
        # Verify ranks 1 through 25 are present
        for rank in range(1, 26):
            if str(rank) not in ranks:
                raise ValueError(f"Rank {rank} is missing from the data")
        
        # If all checks pass, log a success message.
        logging.info("Data validation passed successfully!")
    
    except Exception as error:
        # Log the error with full traceback
        logging.exception("Data validation failed with error: %s", error)
        
        # Optionally: Push error details to XCom or a persistent store for later analysis
        ti.xcom_push(key='validation_error', value=str(error))
        
        # Re-raise the exception so that Airflow marks the task as failed
        raise

    
