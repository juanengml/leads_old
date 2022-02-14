from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
import io

output = io.StringIO()
accounturl = 'DefaultEndpointsProtocol=https;AccountName=prd360v15fs;AccountKey=IfFR5e+a8HmOij0Z0jU9CDOkMYUxBB8of2oJZXSlGnjv8Pdtz5F+jPXnoM3sPIAM++tQuJsp+4/2tsfVOa71HA==;EndpointSuffix=core.windows.net'
chave= 'IfFR5e+a8HmOij0Z0jU9CDOkMYUxBB8of2oJZXSlGnjv8Pdtz5F+jPXnoM3sPIAM++tQuJsp+4/2tsfVOa71HA=='

def delete_folder(FOLDER_NAME):
    blob_service_client = BlobServiceClient.from_connection_string(conn_str=accounturl)
    blob_client = blob_service_client.get_container_client('powerbi')

    for blob in blob_client.list_blobs(name_starts_with=FOLDER_NAME):
        blob_client.delete_blob(blob.name)

def salva_blob(nome_arq):
    delete_folder(nome_arq)
    blob_service_client = BlobServiceClient.from_connection_string(accounturl)
    blob_client = blob_service_client.get_blob_client(container='powerbi', blob = nome_arq)
    
    with open(nome_arq, "rb") as data:
        blob_client.upload_blob(data)
    
    
