

import mysql.connector
import requests
import json
import uuid
import time
from fabric import Connection
import logging

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)


class JbdcMultitableConsumerBenchmarking:

    def __init__(self):
        self.mysql_connection = None
        self.tables = None
        self.table_attribute_dict = dict()
        self.get_mysql_connection()
        self.populate_table_attribute_dict()
        self.request_headers = dict()
        self.get_request_headers()
        self.pipeline_id = "mysqltrashcdccbc98-fc33-41e5-8856-fe23442cb982"
        self.batch_sizes = [100, 1000, 10000]
        self.fetch_sizes = [1000]
        self.partition_sizes = [100, 1000, 10000, 20000]
        self.threads = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        self.scenarios = list()
        self.run_scenarios()

    def get_mysql_connection(self):
        self.mysql_connection = mysql.connector.connect(
            host="deepthought",
            user="root",
            passwd="V1ctoria",
            database="tpcds"
        )

    def get_request_headers(self):
        # authenticate and get SSO auth token
        dpm_auth_creds = {"userName": "alex@woolford.io", "password": "V1ctoria"}
        self.request_headers = {"Content-Type": "application/json", "X-Requested-By": "DPMClient"}
        auth_request = requests.post("http://sch.woolford.io:18631/security/public-rest/v1/authentication/login",
                                     data=json.dumps(dpm_auth_creds), headers=self.request_headers)
        # add SSO token to headers
        self.request_headers["X-Requested-By"] = "DPMClient"
        self.request_headers["X-SS-User-Auth-Token"] = auth_request.cookies["SS-SSO-LOGIN"]
        self.request_headers["X-SS-REST-CALL"] = "true"

    def get_tables(self):
        cursor = self.mysql_connection.cursor()
        sql = 'SHOW TABLES'
        cursor.execute(sql)
        tables = [table[0] for table in cursor.fetchall()]
        self.tables = tables

    def populate_table_attribute_dict(self):

        self.get_tables()

        for table in self.tables:
            cursor = self.mysql_connection.cursor()

            column_count_sql = "SELECT COUNT(*) AS columns FROM information_schema.COLUMNS WHERE table_schema = 'tpcds' AND table_name = '{table}'".format(table=table)
            cursor.execute(column_count_sql)
            column_count = cursor.fetchall()[0][0]

            row_count_sql = "SELECT COUNT(*) AS rows FROM {table}".format(table=table)
            cursor.execute(row_count_sql)
            row_count = cursor.fetchall()[0][0]

            self.table_attribute_dict[table] = {'rows': row_count, 'columns': column_count}

    def run_scenarios(self):
        for table_name in self.table_attribute_dict.keys():
            rows = self.table_attribute_dict[table_name]['rows']
            columns = self.table_attribute_dict[table_name]['columns']
            for batch_size in self.batch_sizes:
                for fetch_size in self.fetch_sizes:
                    for partition_size in self.partition_sizes:
                        for threads in self.threads:
                            scenario = {"table_name": table_name,
                                        "threads": threads,
                                        "batch_size": batch_size,
                                        "fetch_size": fetch_size,
                                        "partition_size": partition_size,
                                        "rows": rows,
                                        "columns": columns,
                                        "runid": str(uuid.uuid4())}
                            if table_name == "customer_demographics":
                                self.scenarios.append(scenario)

        for scenario in self.scenarios:
            self.run_scenario(scenario)

    def run_scenario(self, scenario):
        reset_origin_url = "http://sch.woolford.io:18630/rest/v1/pipeline/{pipeline_id}/resetOffset?rev=0".format(pipeline_id=self.pipeline_id)
        request = requests.post(reset_origin_url, headers=self.request_headers)

        start_pipeline_url = "http://sch.woolford.io:18630/rest/v1/pipeline/{pipeline_id}/start?rev=0".format(pipeline_id=self.pipeline_id)
        request = requests.post(url=start_pipeline_url, data=json.dumps(scenario), headers=self.request_headers)

        finished = False
        while not finished:
            pipeline_status_url = "http://sch.woolford.io:18630/rest/v1/pipeline/{pipeline_id}/status?rev=0".format(pipeline_id=self.pipeline_id)
            request = requests.get(url=pipeline_status_url, headers=self.request_headers)
            if json.loads(request.content)['status'] == "FINISHED":
                finished = True
            time.sleep(1)

        Connection('sch').run('systemctl restart sdc', hide=True)
        time.sleep(45)

    def __del__(self):
        self.mysql_connection.close()


if __name__ == "__main__":
    jbdc_multitable_consumer_benchmarking = JbdcMultitableConsumerBenchmarking()
    del jbdc_multitable_consumer_benchmarking
