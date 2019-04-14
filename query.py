import logging
import os
import time

import boto3
import botocore
from botocore.exceptions import ClientError
import pandas as pd


logger = logging.getLogger('aws-athena-wrapper.query')


class QueryAthena:
    """
    Wrapper class which takes an Athena query and return a pre-processed Pandas data frame
    """
    def __init__(self, athena_bucket, output_bucket, database, table):

        self.athena_client = boto3.client('athena')
        self.s3_client = boto3.client('s3')
        self.s3_bucket = athena_bucket
        self.s3_output = output_bucket
        self.database = database
        self.table = table

    def load_conf(self, query):
        """
        Starts the query execution with a query
        :param query:
        :return: response from boto3
        """
        try:
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={
                    'Database': self.database
                },
                ResultConfiguration={
                    'OutputLocation': self.s3_output,
                }
            )
            print('Execution ID: ' + response['QueryExecutionId'])
        except Exception as e:
            print(e)
            response = None
        return response

    def run_query(self, query):
        """
        Starts the query and watches the status until the execution is completed
        :param query: the query
        :return: response from boto3
        """
        res = self.load_conf(query)
        query_state = None
        while query_state == 'QUEUED' or query_state == 'RUNNING' or query_state is None:
            query_status = self.athena_client.get_query_execution(QueryExecutionId=res["QueryExecutionId"])['QueryExecution']['Status']
            query_state = query_status['State']
            logger.info(query_state)
            if query_state == 'FAILED' or query_state == 'CANCELLED':
                raise Exception('Athena query with the string "{}" stopped with status {}. More details: {}'
                                .format(query, query_state, query_status['StateChangeReason']))
            if query_state == 'SUCCEEDED':
                break
            time.sleep(1)
        print("Query finished.")
        return res

    def create_query(self):
        """
        Creates the query string, runs it and processes the result.
        :return: Pandas data frame with the result
        """
        # Defines the query string
        query = """
        SELECT * FROM {}.{}
        LIMIT 10
        """.format(self.database, self.table)
        res = self.run_query(query)
        df = self.process_result(res)
        return df

    def process_result(self, res):
        """
        Processes the result from Athena. Loads the result CSV in chunks and does part-wise preprocessing
        before concatenating it to one data frame which is returned
        :param res: response from boto3
        :return: pandas data frame
        """
        try:
            # Try to download the result file. Return error if not found. Store temporarily locally.
            file_name = res['QueryExecutionId'] + '.csv'
            self.s3_client.download_file(
                Bucket=self.s3_bucket,
                Key=file_name,
                Filename=file_name,
            )
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print('Simpson export file not found at %s.', file_name)
            else:
                print('Error {} when trying to fetch file {}.'.format(e, file_name))
            return None

        try:
            chunks = pd.read_csv(file_name, chunksize=10000000)
            df_parts = []
            for chunk in chunks:
                df_parts.append(self.chunk_processing(chunk))

            df = pd.concat(df_parts)
        finally:
            os.remove(file_name)
        return df

    @staticmethod
    def chunk_processing(chunk):
        """
        Processing function of the data frame chunks
        :param chunk: chunk of data frame
        :return: processed chunk
        """
        return chunk
