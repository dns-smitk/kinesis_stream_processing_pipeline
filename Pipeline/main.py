import cv2, time
import argparse
from botocore.config import Config
import boto3
from utils.parsing_utils import send_frame_with_signed_request
from utils.parsing_utils import write_to_kinesis
from utils.parsing_utils import upload_frame_to_s3
import botocore
import json
from datetime import datetime
import threading


def start_stream_processing(region, kvs_name, kvs_arn, sm_inf_endpoint, kds_name):
    location = 'HALL'
    bucket_name = 'kinesis-stream-to-frame'
    my_config = Config(
        region_name=region,
    )

    kvs = boto3.client("kinesisvideo", config=my_config)
    # kvs.list_streams()
    endpoint = kvs.get_data_endpoint(
        APIName="GET_HLS_STREAMING_SESSION_URL",
        StreamARN=kvs_arn

        )['DataEndpoint']
    kvam = boto3.client("kinesis-video-archived-media", endpoint_url=endpoint, config=my_config)
    url = kvam.get_hls_streaming_session_url(
        StreamName=kvs_name,
        #PlaybackMode="ON_DEMAND",
        PlaybackMode="LIVE"
        )['HLSStreamingSessionURL']
    vcap = cv2.VideoCapture(url)
    while True:
        ret, frame = vcap.read()
        print('frame processed')
        if ret:
            service = 'sagemaker'  # Update your AWS service
            host = 'runtime.sagemaker.us-east-1.amazonaws.com'  # Update your API host
            endpoint = sm_inf_endpoint
            response_data = send_frame_with_signed_request(frame, service, region, host, endpoint)
            if response_data:
                partition_key = 'cam-1'
                print(response_data)
                kds_response = json.loads(response_data)['output']
                timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
                object_name = f"undetected_frames/{location}/frame_{timestamp}.jpg"
                upload_frame_to_s3(frame, bucket_name, object_name)
                if len(kds_response['boxes']) > 1 :
                    print('detected')
                    
    
                    # Generate the object name by concatenating location with the timestamp
                    object_name = f"detected_frames/{location}/frame_{timestamp}.jpg"
                    # thread_upload_s3 = threading.Thread(
                    #             target=upload_frame_to_s3, 
                    #             args=(
                    #                 frame,
                    #                 bucket_name, 
                    #                 object_name
                    #             )
                    #         )
                    # thread_upload_s3.start()
                    upload_frame_to_s3(frame, bucket_name, object_name)

                    kds_response['location'] = location
                    kds_response['s3_file_name'] = 1
                    kds_response_str = json.dumps(kds_response)
                    data_to_write = kds_response_str.encode('utf-8')
                    write_to_kinesis(kds_name, data_to_write, partition_key)
                    # thread_write_kinesis = threading.Thread(
                    #                 target=write_to_kinesis,
                    #                 args=(
                    #                     kds_name,
                    #                     data_to_write,
                    #                     partition_key
                    #                 )
                    #             )
                    # thread_write_kinesis.start()
                    # thread_upload_s3.join()
                    # thread_write_kinesis.join()



    vcap.release()
def main(region, kvs_name, kvs_arn, sm_inf_endpoint, kds_name) :
    while True :
        try : 
            start_stream_processing(
                region=region,
                kvs_name=kvs_name,
                kvs_arn=kvs_arn,
                sm_inf_endpoint=sm_inf_endpoint,
                kds_name=kds_name
                )
        except Exception as e:
            print(f'Error {e}')
            print('restarting in 5 seconds')
            time.sleep(5)
        

    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Process video stream and send frame to AWS endpoint.")
    parser.add_argument("--kvs_name", required=True, help="Name of the video stream")
    parser.add_argument("--region", required=True, help="AWS region name")
    parser.add_argument("--kvs_arn", required=True, help="ARN of the video stream")
    parser.add_argument("--sm_inf_endpoint", required=True, help="Endpoint to send the frame")
    parser.add_argument("--kds_name", required=True, help="Name of the Kinesis data stream")
    args = parser.parse_args()
    main(
        region=args.region,
        kvs_name=args.kvs_name,
        kvs_arn=args.kvs_arn,
        sm_inf_endpoint=args.sm_inf_endpoint,
        kds_name=args.kds_name
    )
        