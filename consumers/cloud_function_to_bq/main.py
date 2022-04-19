from google.cloud import bigquery
from datetime import datetime
import base64
import uuid
import json
import os

def insert_bq(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    try:
        row = [{
            'TRANSACTION_ID': str(uuid.uuid4()),
            'DATA': str(json.loads(base64.b64decode(event['data']).decode('utf-8'))),
            'SITE': event['attributes']['site'],
            'AUD_INSERTED_AT': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
            'IS_TESTING': False if event['attributes'].get('is_testing') is None else event['attributes'].get('is_testing')
        }]
        print(event)

        bq_client = bigquery.Client(project=os.getenv('PROJECT_ID'))
        table_ref = bq_client.dataset('FEED_RAW').table(os.getenv('LOCATION_NAME'))
        table = bq_client.get_table(table_ref)
        bq_client.insert_rows(table, row)

    except Exception as e:
        raise Exception(f"Error inserting to BQ table. {e}")
