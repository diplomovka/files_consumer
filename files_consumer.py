# created based on: https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/avro_consumer.py

import os
import io
import json
import time
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from serialization_classes.file_data import FileData
import settings
from minio_client import minio_client
from redis_db import get_redis_db


def dict_to_file_data(obj, ctx):
    if obj is None:
        return None

    return FileData(file_name=obj['file_name'],
                    data=obj['data'],
                    data_hash=obj['data_hash'],
                    experiment_name=obj['experiment_name'],
                    last_file=obj['last_file'])


def set_up_consumer():
    schema_registry_conf = {'url': settings.SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    with open(settings.FILES_SCHEMA_PATH, 'r', encoding=settings.AVRO_FILES_ENCODING) as f:
        schema_str = f.read()

    avro_deserializer = AvroDeserializer(schema_registry_client,
                                         schema_str,
                                         dict_to_file_data)
    string_deserializer = StringDeserializer(settings.ENCODING)

    topic = settings.FILES_TOPIC
    consumer_conf = {
        'bootstrap.servers': settings.BOOTSTRAP_SERVERS,
        'key.deserializer': string_deserializer,
        'value.deserializer': avro_deserializer,
        'group.id': settings.GROUP_ID,
        'auto.offset.reset': settings.OFFSET
    }

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([topic])

    return consumer


def create_directory(directory_name):
    if not os.path.exists(directory_name) or not os.path.isdir(directory_name):
        os.mkdir(directory_name)


def create_required_buckets(bucket):
    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)


def get_attributes(file_data):
    return (file_data.file_name, file_data.data, file_data.data_hash)


def store_pointer_data_and_store_file_to_MINIO(data_hash, data, file_name, bucket):
    # minio_client.put_object source: https://www.youtube.com/watch?v=huoXr1pbzUQ
    minio_client.put_object(
        bucket,
        file_name,
        io.BytesIO(data),
        len(data) # learn more: https://min.io/docs/minio/linux/developers/python/API.html#put-object-bucket-name-object-name-data-length-content-type-application-octet-stream-metadata-none-sse-none-progress-none-part-size-0-num-parallel-uploads-3-tags-none-retention-none-legal-hold-false
    )

    pointer = json.dumps({
        'bucket': bucket,
        'file_name': file_name
    }).encode('utf-8')

    # update pointer in redis
    redis_db.set(data_hash, pointer)

    return pointer


def process_file_data(file_data):
    file_name, data, data_hash = get_attributes(file_data)

    pointer = redis_db.get(data_hash)
    # check if chunk is unique
    if pointer is None:
        pointer = store_pointer_data_and_store_file_to_MINIO(
            data_hash, data, file_name, settings.FILES_BUCKET)
    else:
        with open(f'./{settings.EXPERIMENTS_DATA_DIR}/{experiment_name}/{experiment_name}_duplicates.csv', 'a') as file:
            file.write(f'{file_name};{data_hash}\n')


if __name__ == '__main__':
    time.sleep(settings.WAIT_BEFORE_START)

    start_time = time.time()

    consumer = set_up_consumer()
    redis_db = get_redis_db(settings.REDIS_HOST, settings.REDIS_PORT, settings.REDIS_FILES_DB)

    create_directory(settings.EXPERIMENTS_DATA_DIR)

    create_required_buckets(settings.FILES_BUCKET)

    experiment_name = None

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1)
            if msg is None or msg.value() is None:
                continue

            file_data = msg.value()
            experiment_name = file_data.experiment_name

            process_file_data(file_data)

            if file_data.last_file:
                break

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(e)

    consumer.close()

    end_time = time.time()

    with open(f'experiments_data/{experiment_name}/{experiment_name}_consumer_time.txt', 'a') as f:
        f.write(f'Total execution time in seconds: {end_time - start_time}')
