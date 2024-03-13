import cv2
import requests
import datetime
import hashlib
import hmac
from botocore.credentials import create_credential_resolver
from botocore.session import get_session
import numpy as np
from botocore.exceptions import NoCredentialsError
import boto3
from io import BytesIO


def sign(key, msg):
    return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

def get_signature_key(key, date_stamp, region_name, service_name):
    k_date = sign(('AWS4' + key).encode('utf-8'), date_stamp)
    k_region = sign(k_date, region_name)
    k_service = sign(k_region, service_name)
    k_signing = sign(k_service, 'aws4_request')
    return k_signing

def get_signed_header(payload, region, service, host, endpoint) :
    byte_frame = payload
    session = get_session()
    credentials = create_credential_resolver(session).load_credentials()
    access_key = credentials.access_key
    secret_key = credentials.secret_key

    if access_key is None or secret_key is None:
        raise Exception('No access key or secret key available')

    t = datetime.datetime.utcnow()
    amz_date = t.strftime('%Y%m%dT%H%M%SZ')
    date_stamp = t.strftime('%Y%m%d')

    canonical_uri = endpoint
    canonical_querystring = ''
    canonical_headers = 'host:' + host + '\n' + 'x-amz-date:' + amz_date + '\n'
    signed_headers = 'host;x-amz-date'
    payload_hash = hashlib.sha256(byte_frame).hexdigest()
    canonical_request = f"POST\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"

    algorithm = 'AWS4-HMAC-SHA256'
    credential_scope = f"{date_stamp}/{region}/{service}/aws4_request"
    string_to_sign = f"{algorithm}\n{amz_date}\n{credential_scope}\n{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}"

    signing_key = get_signature_key(secret_key, date_stamp, region, service)
    signature = hmac.new(signing_key, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()

    authorization_header = f"{algorithm} Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
    headers = {'x-amz-date': amz_date, 'Authorization': authorization_header, 'Content-Type': 'application/octet-stream'}
    return headers

def send_frame_with_signed_request(frame, service, region, host, endpoint):
    # Encode frame to JPEG
    ret, buffer = cv2.imencode('.jpg', frame)
    if not ret:
        print("Failed to encode frame")
        return
    byte_frame = buffer.tobytes()

    headers = get_signed_header(
        payload=byte_frame,
        region=region,
        service=service,
        host=host,
        endpoint=endpoint
        )
   

    # Send the POST request
    response = requests.post('https://' + host + endpoint, data=byte_frame, headers=headers)
    try:
        response_data = response.text  # Or parse as JSON with response.json() if expected response is JSON
        return response_data
    except Exception as e:
        print(f"Failed to get valid response: {e}")
        return None


def write_to_kinesis(stream_name, data, partition_key):
    kinesis_client = boto3.client('kinesis', region_name='us-east-1')  # Adjust region if necessary
    try:
        response = kinesis_client.put_record(
            StreamName=stream_name,
            Data=data,
            PartitionKey=partition_key
        )
        return {'response' : response, 'error' : None}
    except NoCredentialsError as e:
        return {'response' : None, 'error' : e}
    except Exception as e:
        return {'response' : None, 'error' :e}

def upload_frame_to_s3(frame, bucket_name, object_name):
    _, buffer = cv2.imencode('.jpg', frame)
    frame_bytes = BytesIO(buffer)
    s3_client = boto3.client('s3')
    s3_client.upload_fileobj(frame_bytes, bucket_name, object_name)