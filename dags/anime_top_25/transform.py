from decimal import Decimal

def parse_data(**kwargs):
    # get the raw data from the XCom
    top_25 = kwargs['ti'].xcom_pull(task_ids='extract_top_data', key='raw_data')
    
    res = []
    for data in top_25['data'][0:25]:  # Get only the top 25 entries
        item = {
            'mal_id': data['mal_id'],
            'title': data['title'],
            'url': data['url'],
            'image_url': data['images']['webp']['image_url'],
            'title_english': data.get('title_english'),  # Use .get() to handle missing keys
            'title_japanese': data.get('title_japanese'),
            'licensors': {
                'names': [x['name'] for x in data['licensors']],
                'urls':  [x['url'] for x in data['licensors']]
            },
            'studios': {
                'names': [x['name'] for x in data['studios']],
                'urls': [x['url'] for x in data['studios']]
            },
            'genres': [x['name'] for x in data['genres']],
            'statistics': {
                'rank': str(data['rank']),
                'score': str(data['score']),
                'members': str(data['members']),
                'favorites': str(data['favorites']),
                'scored_by': str(data['scored_by']),
                'popularity': str(data['popularity'])
            }
        }
        res.append(item)
    
    kwargs['ti'].xcom_push(key='parsed_data', value=res)
    return "Data parsed successfully!"