# import os
from minio import Minio
import settings

# NOTE: root_name == access_key, root_pass == secret_key
# source of NOTE: https://stackoverflow.com/questions/67285745/how-can-i-get-minio-access-and-secret-key
# code source: https://www.youtube.com/watch?v=huoXr1pbzUQ
minio_client = Minio(f'{settings.MINIO_HOST}:{settings.MINIO_PORT}',
    access_key=settings.MINIO_ROOT_NAME, secret_key=settings.MINIO_ROOT_PASS, secure=False) # not using SSL
