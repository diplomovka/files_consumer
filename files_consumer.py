# created based on: https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/avro_consumer.py

import io
import json
import time
from pathlib import Path
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from minio.error import S3Error
from serialization_classes.file_data import FileData
import settings
from minio_client import minio_client
from redis_db import get_redis_db


def dict_to_file_data(obj, ctx):
    if obj is None:
        return None

    return FileData(file_name=obj['file_name'],
                    chunk=obj['chunk'],
                    chunk_hash=obj['chunk_hash'],
                    chunk_serial_num=obj['chunk_serial_num'],
                    end_of_file=obj['end_of_file'])


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


def create_required_buckets(buckets):
    for bucket in buckets:
        if not minio_client.bucket_exists(bucket):
            minio_client.make_bucket(bucket)


def get_attributes(file_data):
    return (file_data.file_name, file_data.chunk.decode('utf-8'), file_data.chunk_hash,
            file_data.chunk_serial_num, file_data.end_of_file)


def update_pointer_data(chunk_hash, chunk, chunk_serial_num, file_name, bucket):
    chunk_data = json.dumps({
        'used_in_files': {
            file_name: {'occurences': 1, 'at_indexes': [chunk_serial_num]}
        },
        'data': chunk
    }).encode('utf-8')

    # minio_client.put_object source: https://www.youtube.com/watch?v=huoXr1pbzUQ
    minio_client.put_object(
        bucket,
        f'{chunk_hash}.json',
        io.BytesIO(chunk_data),
        len(chunk_data) # learn more: https://min.io/docs/minio/linux/developers/python/API.html#put-object-bucket-name-object-name-data-length-content-type-application-octet-stream-metadata-none-sse-none-progress-none-part-size-0-num-parallel-uploads-3-tags-none-retention-none-legal-hold-false
    )

    pointer = json.dumps({
        'bucket': bucket,
        'chunk_hash': chunk_hash
    }).encode('utf-8')

    # update pointer in redis
    redis_db.set(chunk_hash, pointer)

    return pointer


def add_chunk_usage(chunk_hash, chunk_serial_num, file_name, bucket):
    object_name = f'{chunk_hash}.json'
    response = minio_client.get_object(bucket, object_name)
    chunk_data = json.load(io.BytesIO(response.data))

    usage_metadata = chunk_data['used_in_files']

    if file_name not in usage_metadata:
        usage_metadata[file_name] = {'occurences': 1, 'at_indexes': [chunk_serial_num]}

    elif chunk_serial_num not in usage_metadata[file_name]['at_indexes']:
        usage_metadata[file_name]['occurences'] += 1
        usage_metadata[file_name]['at_indexes'].append(chunk_serial_num)

    else:
        return False

    chunk_data = json.dumps(chunk_data).encode('utf-8')
    minio_client.put_object(
        bucket,
        object_name,
        io.BytesIO(chunk_data),
        len(chunk_data)
    )

    return True


def remove_chunk_usage(chunk_hash, chunk_serial_num, file_name, bucket):
    object_name = f'{chunk_hash}.json'
    response = minio_client.get_object(bucket, object_name)
    chunk_data = json.load(io.BytesIO(response.data))

    # remove specified usage of this chunk in this file
    usage_metadata_of_file = chunk_data['used_in_files'][file_name]
    usage_metadata_of_file['occurences'] -= 1
    usage_metadata_of_file['at_indexes'].remove(chunk_serial_num)

    if usage_metadata_of_file['occurences'] == 0:
        del chunk_data['used_in_files'][file_name]

    # remove object only, when this was the last usage of it
    if len(chunk_data['used_in_files']) == 0 and usage_metadata_of_file['occurences'] == 0:
        minio_client.remove_object(bucket, object_name)
        redis_db.delete(chunk_hash)
    else:
        chunk_data = json.dumps(chunk_data).encode('utf-8')
        minio_client.put_object(
            bucket,
            object_name,
            io.BytesIO(chunk_data),
            len(chunk_data)
        )


if __name__ == '__main__':
    time.sleep(30)

    consumer = set_up_consumer()
    redis_db = get_redis_db(settings.REDIS_HOST, settings.REDIS_PORT, settings.REDIS_FILES_DB)

    create_required_buckets([settings.FILES_BYTES_BUCKET, settings.FILES_POINTERS_BUCKET])

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1)
            if msg is None or msg.value() is None:
                continue

            file_data = msg.value()

            file_name, chunk, chunk_hash, chunk_serial_num, end_of_file = get_attributes(file_data)

            print(f'{msg.key()} FileChunk {file_name}, {chunk_hash}, {chunk_serial_num}, {end_of_file}')

            pointer = redis_db.get(chunk_hash)
            pointers_change_flag = True
            # check if chunk is unique
            if pointer is None:
                pointer = update_pointer_data(chunk_hash, chunk, chunk_serial_num,
                                            file_name, settings.FILES_BYTES_BUCKET)
            else:
                pointers_change_flag = add_chunk_usage(chunk_hash, chunk_serial_num, file_name, settings.FILES_BYTES_BUCKET)

            if not pointers_change_flag:
                continue

            pointer_decoded = json.load(io.BytesIO(pointer))
            file_name_json_ext = Path(file_name).stem + '.json'

            try:
                # check if file already has pointers
                response = minio_client.get_object(settings.FILES_POINTERS_BUCKET, file_name_json_ext)
                file_data = json.load(io.BytesIO(response.data))
                total_file_pointers = len(file_data['pointers'])
                
                if total_file_pointers <= chunk_serial_num:
                    # content of the file was expanded
                    file_data['pointers'].append(pointer_decoded)

                else:
                    # content of the file was changed
                    replaced_chunk_hash = file_data['pointers'][chunk_serial_num]['chunk_hash']
                    remove_chunk_usage(replaced_chunk_hash, chunk_serial_num, file_name, settings.FILES_BYTES_BUCKET)

                    file_data['pointers'][chunk_serial_num] = pointer_decoded

                    if end_of_file and (total_file_pointers - 1) > chunk_serial_num:
                        # content of the file was shrinked
                        f_pointers = file_data['pointers'][chunk_serial_num + 1:]
                        for i, chunk_pointer in enumerate(f_pointers):
                            f_chunk_serial_num = chunk_serial_num + i + 1
                            remove_chunk_usage(chunk_pointer['chunk_hash'], f_chunk_serial_num, file_name, chunk_pointer['bucket'])

                        # shrink list of chunk pointers
                        file_data['pointers'] = file_data['pointers'][:chunk_serial_num + 1]

                response.close()
                response.release_conn()
            except S3Error as e:
                if e.code == 'NoSuchKey':
                    file_data = {
                        'pointers': [pointer_decoded]
                    }

            if pointers_change_flag and file_data:
                file_data['original_file_name'] = file_name
                file_data = json.dumps(file_data).encode('utf-8')

                # update file pointers in MINIO
                minio_client.put_object(
                    settings.FILES_POINTERS_BUCKET,
                    file_name_json_ext,
                    io.BytesIO(file_data),
                    len(file_data)
                )

        except KeyboardInterrupt:
            break
        except Exception as e:
            print(e)

    consumer.close()
