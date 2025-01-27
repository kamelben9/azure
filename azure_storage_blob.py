import os
from dotenv import load_dotenv
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient

load_dotenv()

SP_ID_SECONDARY=os.getenv("SP_ID_SECONDARY")
SP_SECONDARY_PASSWORD=os.getenv("SP_SECONDARY_PASSWORD")
SP_ID_PRINCIPAL=os.getenv("SP_ID_PRINCIPAL")
TENANT_ID=os.getenv("TENANT_ID")
KEYVAULT_URL=os.getenv("KEYVAULT_URL")
SECRET_NAME=os.getenv("SECRET_NAME")
STORAGE_ACCOUNT_NAME=os.getenv("STORAGE_ACCOUNT_NAME")
CSV_FILE_PATH=os.getenv("CSV_FILE_PATH")
CONTAINER_NAME=os.getenv("CONTAINER_NAME")
BLOB_NAME=os.getenv("BLOB_NAME")

def test():
    # Étape 1 : Récupération du secret depuis le Key Vault
    print("Authentification avec le service principal secondaire...")
    secondary_credential = ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=SP_ID_SECONDARY,
        client_secret=SP_SECONDARY_PASSWORD
    )
    
    secret_client = SecretClient(vault_url=KEYVAULT_URL, credential=secondary_credential)
    print("Récupération du secret...")
    storage_key = secret_client.get_secret(SECRET_NAME).value

    # Étape 2 : Utilisation du service principal primaire pour interagir avec le Data Lake
    print("Authentification avec le service principal primaire...")
    primary_credential = ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=SP_ID_PRINCIPAL,
        client_secret=storage_key
    )

    blob_service_client = BlobServiceClient(
        account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
        credential=primary_credential
    )
    
    # Étape 3 : Téléchargement du fichier dans le Data Lake
    print("Téléchargement du fichier CSV dans le Data Lake...")
    with open(CSV_FILE_PATH, "rb") as data:
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=BLOB_NAME)
        blob_client.upload_blob(data, overwrite=True)
    
    print(f"Fichier {CSV_FILE_PATH} téléchargé avec succès dans {CONTAINER_NAME}/{BLOB_NAME}.")

if __name__ == "__main__":
    test()
