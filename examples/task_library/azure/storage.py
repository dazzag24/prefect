"""
This example illustrates the use of Prefect's Azure Blob Storage tasks.

Each task is instantiated as a template in order to demonstrate how arguments can be
passed either at initialization or when building the flow.
"""
import os
import json
import time
import datetime


import prefect as pf
from prefect.tasks.azure.blobstorage import BlobStorageDownload, BlobStorageUpload, BlobStorageCopy
from prefect.utilities.configuration import set_temporary_config

CONTAINER = "prefect-test"
BLOB = "path/to/blob.json"
#BLOB = "blob.json"
DATA = json.dumps([1, 2, 3])

BLOB_ACCOUNT = os.environ['BLOB_ACCOUNT']
BLOB_SAS = os.environ['BLOB_SAS']
AZ_CREDENTIALS='AZ_CREDS'

# create task templates
upload_template = BlobStorageUpload(container=CONTAINER, azure_credentials_secret=AZ_CREDENTIALS)
download_template = BlobStorageDownload(container=CONTAINER, azure_credentials_secret=AZ_CREDENTIALS)
copy_template = BlobStorageCopy(container=CONTAINER, azure_credentials_secret=AZ_CREDENTIALS, max_retries=3, retry_delay=datetime.timedelta(minutes=1))

with pf.Flow("Azure Example") as flow:
    # upload with default settings
    upl = upload_template(blob_name=BLOB, data=DATA)
    dwl = download_template(blob_name=BLOB, upstream_tasks=[upl])

    # upload to new blob and download it
    upl_new = upload_template(data=DATA, blob_name="another/blob.json")
    dwl_new = download_template(blob_name=upl_new)

    # copy the default blob twice
    cp_1 = copy_template(blob_name=BLOB, target_blob_name="yet/another/blob.json", upstream_tasks=[upl])
    print(cp_1)
    #cp_2 = copy_template(blob_name=cp_1, target_blob_name="one/last/blob.json")

    # download the new blob
    #dwl_new = download_template(blob_name=cp_2)
    dwl_new = download_template(blob_name=cp_1)

with set_temporary_config({"cloud.use_local_secrets": True}):
    with pf.context(secrets=dict(AZ_CREDS={"ACCOUNT_NAME": BLOB_ACCOUNT, "SAS_TOKEN": BLOB_SAS})):
        flow.run()
