import os
import boto3
from datetime import datetime, timedelta
from requests_aws4auth import AWS4Auth
from elasticsearch import Elasticsearch, RequestsHttpConnection
import logging
import curator

logger = logging.getLogger('curator')
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

service = 'es'
credentials = boto3.Session().get_credentials()

def lambda_handler(event, context):

    awsauth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        os.environ['region'],
        service,
        session_token=credentials.token
    )
    es = Elasticsearch(
        hosts = [{'host': os.environ['host'], 'port': 443}],
        http_auth = awsauth,
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection,
        timeout = 120
    )

    utc_yesterday = datetime.utcnow() - timedelta(days=1)
    index_name = os.environ['index_prefix'] + utc_yesterday.strftime(os.environ['date_string'])

    # 同じ名前のSnapshotがすでに存在するときは削除する
    try:
        snapshot_list = curator.SnapshotList(es, repository=os.environ['es_snapshot_repository'])
        snapshot_list.filter_by_regex(kind='prefix', value=index_name)
        curator.DeleteSnapshots(snapshot_list, retry_interval=30, retry_count=3).do_action()
    except curator.exceptions.NoSnapshots as e:
        logging.info("No snapshots to delete")
    except (curator.exceptions.SnapshotInProgress, curator.exceptions.NoSnapshots, curator.exceptions.FailedExecution) as e:
        logging.error("Failed to delete snapshot", exc_info=True)

    # 前日のindexのSnapshotを取る
    try:
        index_list = curator.IndexList(es)
        index_list.filter_by_regex(kind='prefix', value=index_name)
        curator.Snapshot(
            index_list,
            repository = os.environ['es_snapshot_repository'],
            name = index_name,
            wait_for_completion = True
        ).do_action()
    except curator.exceptions.NoIndices as e:
        logging.warning("No indices to take snapshot")
    except (curator.exceptions.SnapshotInProgress, curator.exceptions.FailedExecution) as e:
        logging.error("Failed to take snapshot", exc_info=True)

# for test
if __name__ == '__main__':
    os.environ['host'] = '' # For example, search-my-domain.region.es.amazonaws.com
    os.environ['region'] = '' # For example, us-west-1
    os.environ['es_snapshot_repository'] = ''
    os.environ['index_prefix'] = ''
    os.environ['date_string'] = '' # For example, %Y-%m-%d
    lambda_handler({}, None)
