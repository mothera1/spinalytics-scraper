import boto3
from botocore.config import Config
import sys
import json

#constants
DATABASE_NAME = "spinalytics"
TABLE_NAME = "songs"

class Query:
    
    def __init__(self, client):
        self.client = client
        self.paginator = client.get_paginator('query')
    
    def run_query(self, query_string):
        try:
            tot =[]
            page_iterator = self.paginator.paginate(QueryString=query_string)
            for page in page_iterator:
                self.__parse_query_result(page, tot)
            return {"results" : tot}
        except Exception as err:
            print("Exception while running query:", err)
            


    def __parse_query_result(self, query_result, tot):
        column_info = query_result['ColumnInfo']

        #print("Metadata: %s" % column_info)
        #print("Data: ")
        for row in query_result['Rows']:
            tot.append(self.__parse_row(column_info, row))
        return tot
    
    def __parse_row(self, column_info, row):
        data = row['Data']
        row_output = []
        for j in range(len(data)):
            info = column_info[j]
            datum = data[j]
            row_output.append(self.__parse_datum(info, datum))

        return row_output #"{%s}" % str(row_output)

    def __parse_datum(self, info, datum):
        if datum.get('NullValue', False):
            return ("%s=NULL" % info['Name'])

        column_type = info['Type']

        # If the column is of TimeSeries Type
        if 'TimeSeriesMeasureValueColumnInfo' in column_type:
            return self.__parse_time_series(info, datum)

        # If the column is of Array Type
        elif 'ArrayColumnInfo' in column_type:
            array_values = datum['ArrayValue']
            return ("%s=%s" % (info['Name'], self.__parse_array(info['Type']['ArrayColumnInfo'], array_values)))

        # If the column is of Row Type
        elif 'RowColumnInfo' in column_type:
            row_column_info = info['Type']['RowColumnInfo']
            row_values = datum['RowValue']
            return self.__parse_row(row_column_info, row_values)

        #If the column is of Scalar Type
        else:
            return datum['ScalarValue'] #self.__parse_column_name(info) + datum['ScalarValue']


    def __parse_time_series(self, info, datum):
        time_series_output = []
        for data_point in datum['TimeSeriesValue']:
            time_series_output.append("{time=%s, value=%s}"
                                      % (data_point['Time'],
                                         self.__parse_datum(info['Type']['TimeSeriesMeasureValueColumnInfo'],
                                                            data_point['Value'])))
        return "[%s]" % str(time_series_output)

    def __parse_column_name(self, info):
        if 'Name' in info:
            return info['Name'] + "="
        else:
            return ""

    def __parse_array(self, array_column_info, array_values):
        array_output = []
        for datum in array_values:
            array_output.append(self.__parse_datum(array_column_info, datum))

        return "[%s]" % str(array_output)

def main():
    session = boto3.Session()
    query_client = session.client('timestream-query', config=Config(region_name="us-east-2"))
    q = Query(query_client)
    all = "SELECT * FROM " + DATABASE_NAME + "." + TABLE_NAME + " LIMIT 2" 
    print(json.dumps(q.run_query(all)))
    
        

main()  