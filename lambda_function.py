from __future__ import print_function

import base64
import json
from urllib3 import *

print('Loading function')

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    retries = Retry(total=5,
                    connect=5,
                    read=5,
                    redirect=5,
                    status_forcelist=[500, 503],
                    method_whitelist=['POST'],
                    backoff_factor=0.5,
                    raise_on_redirect=True)
    manager = PoolManager(retries=retries)
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        payload_string = base64.b64decode(record['kinesis']['data'])
        print("Decoded payload: " + payload_string)
        event_package = json.loads(payload_string)
        print("event_package: %s [%s]" % (event_package, type(event_package)))
        target_url = event_package['target_url']
        event_data = event_package['event']
        event_data_string = json.dumps(event_data)
        print("target_url: %s; event_data: %s" % (target_url, event_data_string));
        response = manager.request('POST', target_url, body=event_data_string)
        print("response: %s" % response)
    return 'Successfully processed {} records.'.format(len(event['Records']))
