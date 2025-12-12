from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException # Нужно для вызова исключений в Airflow
import pandas as pd
import requests
import mysql.connector
import json
import random
import time
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from json import loads

# Настройка подключения к базе данных ClickHouse


HOST_SR = BaseHook.get_connection("starrocks_default").host
USER_SR = BaseHook.get_connection("starrocks_default").login
PASSWORD_SR = BaseHook.get_connection("starrocks_default").password
DATABASE_SR = 'stage' 
PORT_SR = BaseHook.get_connection("starrocks_default").port


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 11, 25),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(days=1),
}

# Создадим объект класса DAG
dag =  DAG('mis_loader_DOCTOR'
           , default_args=DEFAULT_ARGS
           , schedule=timedelta(days=1)
           , tags=['mis_parser']
           , max_active_runs=1
           , description="DAG для загрузки данных из MIS Kafka в StarRocks"
           , params={
        "topic_name_param": 'sys__mis__esud_rzdm._opt_esud_esud_99099.fdb.DOCTOR'  # Значение по умолчанию, которое будет выводиться в UI при триггере
        , "prefix_table_name_param": 'MIS_'
        , "suffix_table_name_param": ''
        , "offset_begin_param": -1
        , "offset_end_param": -1
        , "ENV_param": 'TEST'
        , "layer_name_param": "unverified_rdv"
        , "prefix_tbl_layer_name_param": 'sat_rdv_MIS__'
        , "suffix_tbl_layer_name_param": ''
        , "mart_lr_name_param": "unverified_mart"
        , "prefix_tbl_mart_lr_name_param": 'medical__dm_'
        , "suffix_tbl_mart_lr_name_param": '_dim'        
    },
    )

def fetch_data_from_kafka(topic_name, **kwargs):
    
    # Получаем все файлы из Xcom на вчера, если такого ключа нет, то будет возвращен None
    task_instance = kwargs['task_instance']

    topic_name = str(kwargs['dag_run'].conf.get('topic_name_param', topic_name))

    topic_name = topic_name.strip()

    env = kwargs['dag_run'].conf.get('ENV_param', 'TEST')

    if env is None:
        env = ''
    else:
        env = str(kwargs['dag_run'].conf.get('ENV_param', ''))
    
    env = env.strip()
      
    if env == 'TEST':
        consumer = KafkaConsumer(bootstrap_servers=['10.216.237.80:30092', '10.216.237.117:30093', '10.216.237.118:30094', '10.216.237.112:30092'],
                               sasl_mechanism='PLAIN',
                               security_protocol='SASL_PLAINTEXT',
                               sasl_plain_username='admin',
                               sasl_plain_password='Q1w2e3r+',
                               #group_id='$group',
                               auto_offset_reset='earliest',
                               enable_auto_commit=False)
    elif env == 'PROD':
        consumer = KafkaConsumer(bootstrap_servers=['10.216.227.210:30092', '10.216.227.213:30093', '10.216.227.248:30094', '10.216.227.211:30080', '10.216.227.249:30432'],
                               sasl_mechanism='PLAIN',
                               security_protocol='SASL_PLAINTEXT',
                               sasl_plain_username='admin',
                               sasl_plain_password='Q1w2e3r+',
                               #group_id='$group',
                               auto_offset_reset='earliest',
                               enable_auto_commit=False)       
    elif env == 'DEV':
        consumer = KafkaConsumer(bootstrap_servers=['10.216.237.50:30092', '10.216.237.34:30093', '10.216.237.32:30094'],
                               sasl_mechanism='PLAIN',
                               security_protocol='SASL_PLAINTEXT',
                               sasl_plain_username='admin',
                               sasl_plain_password='Q1w2e3r+',
                               #group_id='$group',
                               auto_offset_reset='earliest',
                               enable_auto_commit=False)                                
    else:
        consumer = KafkaConsumer(bootstrap_servers=['kafka-0:9091', 'kafka-1:9091', 'kafka-2:9091'],
                               sasl_mechanism='PLAIN',
                               security_protocol='SASL_PLAINTEXT',
                               sasl_plain_username='admin',
                               sasl_plain_password='Q1w2e3r+',
                               #group_id='$group',
                               auto_offset_reset='earliest',
                               enable_auto_commit=False)


    print(f'topic_name {topic_name}, type(consumer) {type(consumer)}')

    #consumer.subscribe([topic_name])

  
    '''tp = TopicPartition(topic_name,1)
    #register to the topic
    consumer.assign([tp])
    # obtain the last offset value
    consumer.seek_to_end(tp)
    lastOffset = consumer.position(tp)
    consumer.seek_to_beginning(tp)
    for message in consumer:
        print(f"Offset: {message.offset}")
        if message.offset == lastOffset - 1:
            break
    print(f"Конц цикла {lastOffset}")'''

    offset_begin = int(kwargs['dag_run'].conf.get('offset_begin_param', -1))
    offset_end = int(kwargs['dag_run'].conf.get('offset_end_param', -1))

    if offset_begin is None:
        offset_begin = -1
    if offset_end is None:
        offset_end = -1

        # prepare consumer
    tp = TopicPartition(topic_name,0)
    consumer.assign([tp])
    consumer.seek_to_beginning(tp)  

    # obtain the last offset value
    lastOffset = consumer.end_offsets([tp])[tp]

    if offset_end > -1 and offset_end <= lastOffset:
        lastOffset = offset_end + 1

    xcom_keys_lst = []
    i = 0
    for message in consumer:
        if offset_begin != -1 and offset_end != -1:
            if offset_end < message.offset:
                print("Заданный диапазон смещений не существует")
                break

            if not (message.offset >= offset_begin and message.offset <= offset_end):                        
                continue
        
        if message is None or message.value is None:
            print (f"offset {message.offset} message не содержит данных" )
        else:    
            print (f"offset {message.offset} type {type(message)} message {message}" )
            payload=message.value.decode("utf-8")
            mis_data=json.loads(payload)

            xcom_key = 'mis_data_'+ str(i)
            kwargs['ti'].xcom_push(key=xcom_key, value = mis_data)
            xcom_keys_lst.append(xcom_key)
            print(mis_data)
            i = i + 1

        if message.offset == lastOffset - 1:
            break


            #unsubscribe and close  consumer
    consumer.unsubscribe()
    #consumer.unassigns()
    consumer.close()
    kwargs['ti'].xcom_push(key='xcom_keys', value = xcom_keys_lst)
    
    print(kwargs['ti'])
    print(f'task_instance {task_instance}')
    

        
# Функция для загрузки данных в ClickHouse из CSV
def upload_to_storage(table_name_prefix, table_name_postfix, **kwargs):

    
    # Получаем разницу в файлах сегодня и вчера 
    task_instance = kwargs['task_instance']
    print(kwargs['ti'])

    if kwargs['dag_run'].conf.get('prefix_table_name_param', table_name_prefix) is None:
        table_name_prefix = ''
    else:
        table_name_prefix = str(kwargs['dag_run'].conf.get('prefix_table_name_param', table_name_prefix))

    if kwargs['dag_run'].conf.get('suffix_table_name_param', table_name_postfix) is None:
        table_name_postfix = ''
    else:
        table_name_postfix = str(kwargs['dag_run'].conf.get('suffix_table_name_param', table_name_postfix))


    xcom_keys_lst =   kwargs['ti'].xcom_pull(key='xcom_keys', task_ids="fetch_data_from_kafka")
    print(f'xcom_keys_lst {xcom_keys_lst}')

    for xcon_key in xcom_keys_lst:

        mis_data = kwargs['ti'].xcom_pull(key=xcon_key, task_ids="fetch_data_from_kafka")
        print(f'xcon_key {xcon_key}')
        
        if mis_data is None:
            raise ValueError('mis_data пустое значение')
        
        print(f'mis_data type {type(mis_data)}')
        #2. Парсинг сообщения
        result = []
        result2 = []      

        new_disct = {}

        table_nm = ''

        payload_data = mis_data['payload']
        for key,val in payload_data.items():
            if key == 'op' or key == 'ts_ms':
                new_disct[key] = val

            if key == 'source':
                for key_s,val_s in payload_data[key].items():
                    if key_s == 'ts_ms':
                        new_disct['source_ts_ms'] = val[key_s]
                    if key_s ==  'table':
                        table_nm = val[key_s]

            if mis_data['payload']['op'] == 'DELETE':
                if key == 'before':
                    for key_b, val_b in payload_data[key].items():
                        new_disct[key_b] = val_b 
                    new_disct['deleted_flg'] = 'Y'
            else:
                if key == 'after':
                    for key_a, val_a in payload_data[key].items():
                        new_disct[key_a] = val_a
                    new_disct['deleted_flg'] = 'N'


        result2.append(list(new_disct.keys()))
        result2.append(list(new_disct.values()))    
        
        column = list(new_disct.keys())
        row = list(new_disct.values())
        prefix = table_name_prefix
        postfix = table_name_postfix  

        column_str = ','.join(column)
        value_subst_str = '%s, '*len(column)
        value_subst_str = value_subst_str[0:len(value_subst_str)-2]

        cnx = mysql.connector.connect(user=USER_SR, password=PASSWORD_SR,
                                host=HOST_SR,
                                port = PORT_SR,
                                database=DATABASE_SR)
         

        cursor = cnx.cursor()  

        table_nm = prefix+table_nm+postfix

        print(f"insert into {table_nm}({column_str}) VALUES ({value_subst_str})" )
        print(tuple(row))

        sql = f"insert into {table_nm}({column_str}) VALUES ({value_subst_str})"  
        values = tuple(row) 
        cursor.execute(sql, values)  
        cnx.commit()  

# Функция для загрузки данных в ClickHouse из CSV
def upload_to_layer(layer_name, table_name_prefix, table_name_postfix, **kwargs):

    
    # Получаем разницу в файлах сегодня и вчера 
    task_instance = kwargs['task_instance']
    print(kwargs['ti'])

    if kwargs['dag_run'].conf.get('layer_name_param', layer_name) is None:
       raise ValueError('layer_name_param не задан') 
    else:
       DATABASE_SR = str(kwargs['dag_run'].conf.get('layer_name_param', layer_name))
     
    if kwargs['dag_run'].conf.get('prefix_tbl_layer_name_param', table_name_prefix) is None:
        table_name_prefix = ''
    else:
        table_name_prefix = str(kwargs['dag_run'].conf.get('prefix_tbl_layer_name_param', table_name_prefix))

    if kwargs['dag_run'].conf.get('suffix_tbl_layer_name_param', table_name_postfix) is None:
        table_name_postfix = ''
    else:
        table_name_postfix = str(kwargs['dag_run'].conf.get('suffix_tbl_layer_name_param', table_name_postfix))



    xcom_keys_lst =   kwargs['ti'].xcom_pull(key='xcom_keys', task_ids="fetch_data_from_kafka")
    print(f'xcom_keys_lst {xcom_keys_lst}')

    for xcon_key in xcom_keys_lst:

        mis_data = kwargs['ti'].xcom_pull(key=xcon_key, task_ids="fetch_data_from_kafka")
        print(f'xcon_key {xcon_key}')
        
        if mis_data is None:
            raise ValueError('mis_data пустое значение')
        
        print(f'mis_data type {type(mis_data)}')
        #2. Парсинг сообщения
        result = []
        result2 = []      

        new_disct = {}

        table_nm = ''

        payload_data = mis_data['payload']
        for key,val in payload_data.items():
            if key == 'op' or key == 'ts_ms':
                new_disct[key] = val

            if key == 'source':
                for key_s,val_s in payload_data[key].items():
                    if key_s == 'ts_ms':
                        new_disct['source_ts_ms'] = val[key_s]
                    if key_s ==  'table':
                        table_nm = val[key_s]

            if mis_data['payload']['op'] == 'DELETE':
                if key == 'before':
                    for key_b, val_b in payload_data[key].items():
                        new_disct[key_b] = val_b 
                    new_disct['deleted_flg'] = 'Y'
            else:
                if key == 'after':
                    for key_a, val_a in payload_data[key].items():
                        new_disct[key_a] = val_a
                    new_disct['deleted_flg'] = 'N'


        '''result2 = list(new_disct.keys()) + list(new_disct.values())'''

        result2.append(list(new_disct.keys()))
        result2.append(list(new_disct.values()))    
        
        column = list(new_disct.keys())
        row = list(new_disct.values())
        prefix = table_name_prefix
        postfix = table_name_postfix  

        column_str = ','.join(column)
        value_subst_str = '%s, '*len(column)
        value_subst_str = value_subst_str[0:len(value_subst_str)-2]


        cnx = mysql.connector.connect(user=USER_SR, password=PASSWORD_SR,
                                host=HOST_SR,
                                port = PORT_SR,
                                database=DATABASE_SR)
         

        cursor = cnx.cursor()  

        table_nm = prefix+table_nm+postfix

        print(f"insert into {table_nm}({column_str}) VALUES ({value_subst_str})" )
        print(tuple(row))

        sql = f"insert into {table_nm}({column_str}) VALUES ({value_subst_str})"  
        values = tuple(row) 
        cursor.execute(sql, values)  
        cnx.commit()  

def upload_to_mart(mart_lr_name, table_name_prefix, table_name_postfix, **kwargs):

    
    # Получаем разницу в файлах сегодня и вчера 
    task_instance = kwargs['task_instance']
    print(kwargs['ti'])

    if kwargs['dag_run'].conf.get('mart_lr_name_param', mart_lr_name) is None:
       raise ValueError('mart_lr_name_param не задан') 
    else:
       DATABASE_SR = str(kwargs['dag_run'].conf.get('mart_lr_name_param', mart_lr_name))
     
    if kwargs['dag_run'].conf.get('prefix_tbl_mart_lr_name_param', table_name_prefix) is None:
        table_name_prefix = ''
    else:
        table_name_prefix = str(kwargs['dag_run'].conf.get('prefix_tbl_mart_lr_name_param', table_name_prefix))

    if kwargs['dag_run'].conf.get('suffix_tbl_mart_lr_name_param', table_name_postfix) is None:
        table_name_postfix = ''
    else:
        table_name_postfix = str(kwargs['dag_run'].conf.get('suffix_tbl_mart_lr_name_param', table_name_postfix))



    xcom_keys_lst =   kwargs['ti'].xcom_pull(key='xcom_keys', task_ids="fetch_data_from_kafka")
    print(f'xcom_keys_lst {xcom_keys_lst}')

    for xcon_key in xcom_keys_lst:

        mis_data = kwargs['ti'].xcom_pull(key=xcon_key, task_ids="fetch_data_from_kafka")
        print(f'xcon_key {xcon_key}')
        
        if mis_data is None:
            raise ValueError('mis_data пустое значение')
        
        print(f'mis_data type {type(mis_data)}')
        #2. Парсинг сообщения
        result = []
        result2 = []      

        new_disct = {}

        table_nm = ''

        payload_data = mis_data['payload']
        for key,val in payload_data.items():
            if key == 'op' or key == 'ts_ms':
                new_disct[key] = val

            if key == 'source':
                for key_s,val_s in payload_data[key].items():
                    if key_s == 'ts_ms':
                        new_disct['source_ts_ms'] = val[key_s]
                    if key_s ==  'table':
                        table_nm = val[key_s]

            if mis_data['payload']['op'] == 'DELETE':
                if key == 'before':
                    for key_b, val_b in payload_data[key].items():
                        new_disct[key_b] = val_b 
                    new_disct['deleted_flg'] = 'Y'
            else:
                if key == 'after':
                    for key_a, val_a in payload_data[key].items():
                        new_disct[key_a] = val_a
                    new_disct['deleted_flg'] = 'N'


        '''result2 = list(new_disct.keys()) + list(new_disct.values())'''

        result2.append(list(new_disct.keys()))
        result2.append(list(new_disct.values()))    
        
        column = list(new_disct.keys())
        row = list(new_disct.values())
        prefix = table_name_prefix
        postfix = table_name_postfix  

        column_str = ','.join(column)
        value_subst_str = '%s, '*len(column)
        value_subst_str = value_subst_str[0:len(value_subst_str)-2]


        cnx = mysql.connector.connect(user=USER_SR, password=PASSWORD_SR,
                                host=HOST_SR,
                                port = PORT_SR,
                                database=DATABASE_SR)
         

        cursor = cnx.cursor()  

        table_nm = prefix+table_nm+postfix

        print(f"insert into {table_nm}({column_str}) VALUES ({value_subst_str})" )
        print(tuple(row))

        sql = f"insert into {table_nm}({column_str}) VALUES ({value_subst_str})"  
        values = tuple(row) 
        cursor.execute(sql, values)  
        cnx.commit() 



fetch_data_from_kafka = PythonOperator(
    task_id='fetch_data_from_kafka',
    python_callable=fetch_data_from_kafka,
    op_args = ['sys__mis__esud_rzdm._opt_esud_esud_99099.fdb.DOCTOR'],
    dag=dag,
)

# Задачи для загрузки данных 
upload_to_storage = PythonOperator(
    task_id='upload_to_storage',
    python_callable=upload_to_storage,
    op_args = ['MIS_', ''],
    dag=dag,
)

upload_to_layer = PythonOperator(
    task_id='upload_to_layer',
    python_callable=upload_to_layer,
    op_args = ['unverified_rdv', 'sat_rdv_MIS__', ''],
    dag=dag,
)

upload_to_mart = PythonOperator(
    task_id='upload_to_mart',
    python_callable=upload_to_mart,
    op_args = ['unverified_mart', 'medical__dm_', '_dim'],
    dag=dag,
)


fetch_data_from_kafka >> upload_to_storage >> upload_to_layer >> upload_to_mart