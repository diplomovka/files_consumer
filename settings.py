import os

REDIS_HOST = str(os.getenv('REDIS_HOST') or 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT') or 6379)
REDIS_DB = int(os.getenv('REDIS_DB') or 0)
REDIS_FILES_DB = int(os.getenv('REDIS_FILES_DB') or 1)

MINIO_HOST = str(os.getenv('MINIO_HOST') or 'localhost')
MINIO_PORT = str(os.getenv('MINIO_PORT') or '9000')
MINIO_ROOT_NAME = str(os.getenv('MINIO_ROOT_USER') or 'root')
MINIO_ROOT_PASS = str(os.getenv('MINIO_ROOT_PASSWORD') or 'password')

FILES_BYTES_BUCKET = str(os.getenv('FILES_BYTES_BUCKET') or 'files-bytes')
FILES_POINTERS_BUCKET = str(os.getenv('FILES_POINTERS_BUCKET') or 'files-pointers')

FILES_TOPIC = str(os.getenv('FILES_TOPIC') or 'FILES_TOPIC')

FILES_SCHEMA_PATH = str(os.getenv('FILES_SCHEMA_PATH') or './avro_files/files.avsc')

BOOTSTRAP_SERVERS = str(os.getenv('BOOTSTRAP_SERVERS') or 'localhost:9092')
SCHEMA_REGISTRY_URL = str(os.getenv('SCHEMA_REGISTRY_URL') or 'http://localhost:8085')
GROUP_ID = str(os.getenv('GROUP_ID') or 'data_lake')
OFFSET = str(os.getenv('OFFSET') or 'earliest')
ENCODING = str(os.getenv('ENCODING') or 'utf_8')
AVRO_FILES_ENCODING = str(os.getenv('AVRO_FILES_ENCODING') or 'utf-8')

EXPERIMENTS_DATA_DIR = 'experiments_data'

WAIT_BEFORE_START = int(os.getenv('WAIT_BEFORE_START') or 30)