import boto3
import os

# Konfiguracja
SOURCE_BUCKET = "twoj-bucket-zrodlowy"
SOURCE_KEY = "sciezka/do/pliku.csv"
TARGET_BUCKET = "twoj-bucket-docelowy"
TARGET_KEY = "sciezka/do/nowej-nazwy-pliku.csv"

# Inicjalizacja klienta S3
s3 = boto3.client('s3')


def copy_csv():
   try:
       # Pobranie pliku z S3
       response = s3.get_object(Bucket=SOURCE_BUCKET, Key=SOURCE_KEY)
       data = response['Body'].read()

       # Zapisanie pliku z nową nazwą w S3
       s3.put_object(Bucket=TARGET_BUCKET, Key=TARGET_KEY, Body=data)
       print(f"Plik skopiowany z {SOURCE_BUCKET}/{SOURCE_KEY} do {TARGET_BUCKET}/{TARGET_KEY}")
   except Exception as e:
       print(f"Błąd podczas kopiowania pliku: {e}")


copy_csv()