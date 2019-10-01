import os
import json
import azure.storage.blob as asb
from dotenv import load_dotenv

load_dotenv(verbose=True)

#docker pull arafato/azurite
#mkdir ~/blob_emulator
#docker run -e executable=blob -d -t -p 10000:10000 -v ~/blob_emulator:/opt/azurite/folder arafato/azurite

BLOB_ACCOUNT = os.environ['BLOB_ACCOUNT']
BLOB_SAS = os.environ['BLOB_SAS']
#CONTAINER="prefect-test"
CONTAINER="devstoreaccount1"
BLOB = "blob.json"
DATA = json.dumps([1, 2, 3])

#blob_service = blobsrv.BlockBlobService(account_name=BLOB_ACCOUNT, sas_token=BLOB_SAS)
blob_service = asb.BlockBlobService(is_emulated=True)
                
blob_service.create_blob_from_text(container_name=CONTAINER, blob_name=BLOB, text=DATA)

blob_result = blob_service.get_blob_to_text(container_name=CONTAINER, blob_name=BLOB)

blob_url = blob_service.make_blob_url(CONTAINER, BLOB)

target_container = CONTAINER
target_blob_name = "blob1.json"

print("Copying from {} to {}/{}".format(blob_url, target_container, target_blob_name))

blob_service.copy_blob(
    target_container,
    target_blob_name,
    blob_url
    #f"https://{az_account_name}.blob.core.windows.net/{container}/{blob_name}"
)
