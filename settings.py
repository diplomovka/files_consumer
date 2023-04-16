import os

MAX_WORKERS = int(os.getenv('MAX_WORKERS') or 8)

HBASE_HOST = str(os.getenv('HBASE_HOST') or 'localhost')
HBASE_PORT = int(os.getenv('HBASE_PORT') or 9090)
HBASE_TABLE_NAME = str(os.getenv('HBASE_TABLE_NAME') or 'files-bytes-pointers')
HBASE_COLUMN_FAMILY_NAME = str(os.getenv('HBASE_COLUMN_FAMILY_NAME') or 'column-family')

MINIO_HOST = str(os.getenv('MINIO_HOST') or 'localhost')
MINIO_PORT = str(os.getenv('MINIO_PORT') or '9000')
MINIO_ROOT_NAME = str(os.getenv('MINIO_ROOT_USER') or 'root')
MINIO_ROOT_PASS = str(os.getenv('MINIO_ROOT_PASSWORD') or 'password')

FILES_BYTES_BUCKET = str(os.getenv('FILES_BYTES_BUCKET') or 'files-bytes')
FILES_POINTERS_BUCKET = str(os.getenv('FILES_POINTERS_BUCKET') or 'files-pointers')

FILES_TOPIC = str(os.getenv('FILES_TOPIC') or 'FILES_TOPIC')

FILES_SCHEMA_PATH = str(os.getenv('FILES_SCHEMA_PATH') or './avro_files/files_array.avsc')

BOOTSTRAP_SERVERS = str(os.getenv('BOOTSTRAP_SERVERS') or 'localhost:9092')
SCHEMA_REGISTRY_URL = str(os.getenv('SCHEMA_REGISTRY_URL') or 'http://localhost:8085')
GROUP_ID = str(os.getenv('GROUP_ID') or 'data_lake')
OFFSET = str(os.getenv('OFFSET') or 'earliest')
ENCODING = str(os.getenv('ENCODING') or 'utf_8')
AVRO_FILES_ENCODING = str(os.getenv('AVRO_FILES_ENCODING') or 'utf-8')

EXPERIMENTS_DATA_DIR = 'experiments_data'

WAIT_BEFORE_START = int(os.getenv('WAIT_BEFORE_START') or 45)