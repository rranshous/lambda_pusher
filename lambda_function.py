from __future__ import print_function

import base64
import json
import time
from urllib3 import *

print('Loading function')

def lambda_handler(event, context):
    start_time = time.time()
    #print("Received event: " + json.dumps(event, indent=2))
    retries = Retry(total=5,
                    connect=5,
                    read=5,
                    redirect=5,
                    status_forcelist=[500, 503],
                    method_whitelist=['POST'],
                    backoff_factor=0.5,
                    raise_on_redirect=True)
    manager = PoolManager(retries=retries)
    total = len(event['Records'])
    count = 0
    print("Total events: %s" % total)
    for record in event['Records']:
        print("Record: %s, TimeSince: %s" % (record, time.time() - start_time))
        count = count + 1
        #print("handling %s of %s" % (count, total))
        # Kinesis data is base64 encoded so decode here
        payload_string = base64.b64decode(record['kinesis']['data'])
        shard_id = record['kinesis']['partitionKey']
        #print("Decoded payload: " + payload_string)
        target_url = 'http://ranshous.com:4567'
        event_data_string = payload_string
        print("[%s|%s] target_url: %s" % (shard_id, event_data_string, target_url))

        try:
            response = manager.request('POST', target_url, body=event_data_string, headers={'JMSXGroupID':str(shard_id)})
        except Exception as e:
            print("[%s|%s] Exception occurred: %s" % (shard_id, event_data_string, e))
            raise

        print("[%s|%s] response: %s" % (response.status, shard_id, event_data_string))
    return 'Successfully processed {} records.'.format(len(event['Records']))
