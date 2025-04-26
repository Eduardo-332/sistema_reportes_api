import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from datetime import datetime, timedelta

load_dotenv()

AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_SAK")
AZURE_STORAGE_CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER")


class ABlob:
    def __init__(self):
        self.blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        self.container_client = self.blob_service_client.get_container_client(AZURE_STORAGE_CONTAINER)

    def generate_sas(self, id: int):
        blob_name = f"poke_report_{id}.csv"
        sas_token = generate_blob_sas(
            account_name=self.blob_service_client.account_name,
            container_name=AZURE_STORAGE_CONTAINER,
            blob_name=blob_name,
            account_key=self.blob_service_client.credential.account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(hours=1)
        )
        return sas_token
    
    def delete_blob(self, id: int):
        try:
            blob_name = f"poke_report_{id}.csv"
            print(f"Intentando eliminar blob: {blob_name}")
            
            
            found = False
            blobs = list(self.container_client.list_blobs(name_starts_with=blob_name))
            for blob in blobs:
                if blob.name == blob_name:
                    found = True
                    print(f"Blob encontrado: {blob_name}")
                    break
            
            if not found:
                print(f"No se encontró ningún blob con el nombre {blob_name}")
                return False
            
            # Eliminar el blob
            blob_client = self.container_client.get_blob_client(blob_name)
            blob_client.delete_blob()
            print(f"Blob {blob_name} eliminado exitosamente")
            return True
            
        except Exception as e:
            print(f"Error al eliminar blob {blob_name}: {str(e)}")
            raise e
        


