import json
from dataclasses import asdict

from realestate_com_au import RealestateComAu

import boto3

from datetime import datetime, timedelta, date
from botocore.exceptions import ClientError
import os
import json

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client(
        "s3"
    )

    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        print(e)
        return False
    return True


def handler(event, context):

    bucket_name = "data-lake-849465484312"

    api = RealestateComAu()

    # Get property listings
    listings = api.search(
        locations=["nsw"],
        channel="buy",
        sort_type="new-desc",
        limit=300
    )

    temp_loc = "/tmp/listings.json"

    today = datetime.utcnow()

    with open(temp_loc, "w", encoding='utf-8') as f:
        out_str_list = []

        for listing in listings:
            dict_listing = asdict(listing)

            # Remove unwanted keys
            dict_listing.pop('images', None)
            dict_listing.pop('images_floorplans', None)
            dict_listing.pop('listers', None)

            # Add load date
            dict_listing['load_date'] = today.isoformat()

            json_str = json.dumps(dict_listing)
            out_str_list.append(json_str)

        f.write('\n'.join(out_str_list))

    
    s3_prefix = (
        f"raw/realestate-com-au/buy-listings/{today.strftime('year=%Y/month=%m/day=%d/hour=%H/minute=%M/second=%S')}/"
    )

    upload_file(
        temp_loc,
        bucket_name,
        object_name=s3_prefix + "buy-listings.json",
    )

    return {
        "statusCode": 200,
        "body": json.dumps(
            f"Successfully created {s3_prefix}buy-listings.json"
        ),
    }

# if __name__ == "__main__":
#     print(handler(1,1))