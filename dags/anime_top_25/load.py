import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime, timezone
from decimal import Decimal
import os



AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_KEY")

# load 3 tables
dynamodb = boto3.resource(
        "dynamodb",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name="us-west-2"
    )
facts_table = dynamodb.Table("anime_top_25_facts")
anime_dimension = dynamodb.Table("anime_dimension")
top_25_rank = dynamodb.Table("top_25_rank_history")

def load_data(**kwargs):
    snapshot = kwargs['ti'].xcom_pull(task_ids='transform_top_data', key='parsed_data')
    snapshot_timestamp = get_timestamp() # timestamp of the snapshot
    
    for item in snapshot:
        mal_id = item.get('mal_id') # mal_id of the anime
        
        print(f"Handling facts table for mal_id: {mal_id}")
        # handle facts table
        update_fact_table(mal_id, snapshot_timestamp, item.get('statistics'))
        
        print(f"Handling anime dimension table for mal_id: {mal_id}")
        # handle anime dimension table
        update_anime_dimension_item(mal_id, item)
            
        print(f"Handling top 25 rank table for mal_id: {mal_id}")
        # handle top 25 rank table (SCD)
        item_rank = item.get('statistics', {}).get('rank')
        if item_rank is not None:    
            update_top_25_rank(mal_id, item_rank, snapshot_timestamp)
        else:
            raise ValueError(f"Rank is missing for mal_id: {mal_id}")
        
    
    


def get_timestamp():
    timestamp = datetime.now(timezone.utc).isoformat(timespec='seconds').replace("+00:00", "Z")
    return timestamp

def update_fact_table(mal_id, snapshot_timestamp, statisitcs):
    if mal_id is None or statisitcs is None:
        print("mal_id or statistics is None")
        return
    item = {
        'mal_id': mal_id,
        'snapshot_timestamp': snapshot_timestamp,
        'favorites': Decimal(statisitcs['favorites']),
        'scored_by': Decimal(statisitcs['scored_by']),
        'members': Decimal(statisitcs['members']),
        'popularity': Decimal(statisitcs['popularity']),
        'score': Decimal(statisitcs['score']),
    }
    facts_table.put_item(Item=item)
    print(f"Inserted new record for mal_id: {mal_id}")
    return

def update_anime_dimension_item(mal_id, snapshot_item):
    if mal_id is None or snapshot_item is None:
        print("mal_id or snapshot_item is None")
        return
    
    # check if mal_id already exists in the table
    response = anime_dimension.query(
        KeyConditionExpression=Key('mal_id').eq(mal_id)
    )
    
    # if mal_id does not exist, insert a new record
    if response['Count'] == 0:
        item = {
            'mal_id': mal_id,
            'title': snapshot_item['title'],
            'title_english': snapshot_item['title_english'],
            'title_japanese': snapshot_item['title_japanese'],
            'image_url': snapshot_item['image_url'],
            'url': snapshot_item['url'],
            'genres': snapshot_item['genres'],
            'licensors_names': snapshot_item['licensors']['names'],
            'licensors_urls': snapshot_item['licensors']['urls'],
            'studios_names': snapshot_item['studios']['names'],
            'studios_urls': snapshot_item['studios']['urls'],
        }
        anime_dimension.put_item(Item=item)
        print(f"Inserted new record for mal_id: {mal_id}")
        return
    print(f"Record already exists for mal_id: {mal_id}")
    return


# SCD Type 2 handling
def update_top_25_rank(mal_id, item_rank, snapshot_timestamp):
    # query to get the current rank
    response = top_25_rank.query(
        KeyConditionExpression=Key('mal_id').eq(mal_id),
        FilterExpression=Attr('current_flag').eq(True)
    )
    
    # get the current record
    current_record = response.get('Items', [])
    
    # if there is no current record, insert a new record
    if len(current_record) == 0:
        item = {
            'mal_id': mal_id,
            'rank': item_rank,
            'effective_date': snapshot_timestamp,
            'end_date': None,
            'current_flag': True
        }
        top_25_rank.put_item(Item=item)
        print(f"Inserted new record for mal_id: {mal_id} with rank: {item_rank}")
        return
    
    # if there is a current record, get the current record
    current_record = current_record[0]

    # check if the rank has changed
    if current_record['rank'] != item_rank:
        # update the current record
        top_25_rank.update_item(
            Key={
                'mal_id': mal_id,
                'effective_date': current_record['effective_date']
            },
            UpdateExpression="SET current_flag = :f, end_date = :ed",
            ExpressionAttributeValues={
                ':f': False,
                ':ed': snapshot_timestamp
            }
        )
        
        # insert a new record with updated rank
        item = {
            'mal_id': mal_id,
            'rank': item_rank,
            'effective_date': snapshot_timestamp,
            'end_date': None,
            'current_flag': True
        }
        top_25_rank.put_item(Item=item)
        print(f"Updated record for mal_id: {mal_id}")
    else:
        print(f"Rank has not changed for mal_id: {mal_id}")
    
    return
        