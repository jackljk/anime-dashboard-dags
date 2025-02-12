import requests

def get_top_data(**kwargs):
    url = "https://api.jikan.moe/v4/top/anime"
    try:
        response = requests.get(url)
        response.raise_for_status()
        kwargs['ti'].xcom_push(key='raw_data', value=response.json())
        return "Data extracted successfully!"
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Error fetching data from Jikan API: {str(e)}")
