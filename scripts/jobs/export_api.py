from flask import Flask, jsonify
from flask_restful import Resource, Api, reqparse
import pandas as pd
import sys

import glob
import datetime as dt
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType,StructType,StructField
from scripts.utils.utils_file import *
from scripts.utils.utils_pyspark import *


data_struct = StructType(
    [
        StructField("PAYS", StringType(), True),
        StructField("C_RISK", StringType(), True),
        StructField("C_DATE", StringType(), True),
        StructField("C_SOURCE", StringType(), True),
        StructField("DT_SOURCE", StringType(), True),

        ]
)
app = Flask(__name__)
api = Api(app)

data_arg = reqparse.RequestParser()
data_arg.add_argument("PAYS", type=str, help="Enter PAYS")
data_arg.add_argument("C_RISK", type=str, help="Enter C_RISK")

# Get config user
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(ROOT_DIR, '../conf' + os.path.sep + 'config_user.yaml')
config_user = read_yaml_file(config_path)

# Get file path of output
output_path = config_user.get('transformations_path')
output_path_path_file = create_file_path(output_path, config_user)

# Get file path of output
export_path = config_user.get('export_path')
export_path_file = create_file_path(export_path, config_user)

file = glob.glob(output_path_path_file + os.path.sep + "*.parquet")[0]

from flask import Flask
from flask_restful import Resource, Api, reqparse
import pandas as pd

app = Flask(__name__)
api = Api(app)

spark = SparkSession \
    .builder \
    .appName("SparkSessionEX") \
    .config("spark.debug.maxToStringFields", "50") \
    .getOrCreate()


class Pays(Resource):
    def get(self):
        data = read_parquet_spark(spark, output_path_path_file)
        show = data.collect()
        return {'knowyourcountry.com': show}, 200  # return data dict and 200 OK

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('text', help='Type in some text')
        args = parser.parse_args()
        return {"Answer": f"You said: {args['text']}"}
    """
    def post(self):
        # initialize parser
        parser = reqparse.RequestParser()
        parser.add_argument('PAYS', required=True)
        parser.add_argument('C_RISK', required=True)

        args = parser.parse_args()  # parse arguments to dictionary

    
        # read our CSV
        data = read_parquet_spark(spark, output_path_path_file)

        # check if Pays already exists
        if args['PAYS'] in list(data.select(data.PAYS).toPandas()['PAYS']):
            # if PAYS already exists, return 401 unauthorized
            return {
                       'message': f"'{args['PAYS']}' already exists."
                   }, 409
        elif args['C_RSIK'] not in ['grey list', 'black list', 'white list']:
            return {
                       'message': f"'{args['C_RISK']}' not recognized."
                   }, 409

        else:
            # otherwise, we can add the new ID record
            # create new dataframe containing new values
            today = dt.datetime.today().strftime('"%Y-%m-%d"')
            columns = ['PAYS', 'C_RISK', 'C_DATE', 'C_SOURCE', 'DT_SOURCE']
            value = [(args['PAYS'], args['C_RISK'], today, 'Source_user', today)]
            new_data = spark.createDataFrame(data = value, schema=data_struct)

            # add the newly provided values
            new_df = data.union(new_data)
            create_dir(export_path_file)
            new_df.write.mode('overwrite').parquet(export_path_file)
            # data.to_csv('C_PAYS.csv', index=False)  # save back to CSV
            return {'new data': new_df.collect()}, 200  # return data with 200 OK
    
        return {
                   'message': "hello"
               }, 200
         """

    def patch(self):
        # initialize parser
        parser = reqparse.RequestParser()
        # add args
        parser.add_argument('ID', required=True, type=int)
        # name/rating are optional
        parser.add_argument('PAYS', store_missing=False)
        parser.add_argument('C_RISK', store_missing=False)
        # parse arguments to dictionary
        args = parser.parse_args()

        # read our CSV
        data = pd.read_csv(file)

        # check that the ID exists
        if args['ID'] in list(data['ID']):
            # if it exists, we can update it, first we get user row
            user_data = data[data['ID'] == args['ID']]

            # if name has been provided, we update name
            if 'PAYS' in args:
                user_data['PAYS'] = args['PAYS']
            # if rating has been provided, we update rating
            if 'C_RISK' in args:
                user_data['C_RISK'] = args['C_RISK']

            # update data
            data[data['ID'] == args['ID']] = user_data
            # now save updated data
            data.to_csv('C_PAYS.csv', index=False)
            # return data and 200 OK
            return {'data': data.to_dict()}, 200

        else:
            # otherwise we return 404 not found
            return {
                       'message': f"'{args['ID']}' ID does not exist."
                   }, 404

    def delete(self):
        parser = reqparse.RequestParser()  # initialize parser
        parser.add_argument('ID', required=True, type=int)  # add ID arg
        args = parser.parse_args()  # parse arguments to dictionary

        # read our CSV
        data = pd.read_csv('C_PAYS.csv')

        # check that the ID exists
        if args['ID'] in list(data['ID']):
            # if it exists, we delete it
            data = data[data['ID'] != args['ID']]
            # save the data
            data.to_csv('C_PAYS.csv', index=False)
            # return data and 200 OK
            return {'data': data.to_dict()}, 200

        else:
            # otherwise we return 404 not found
            return {
                'message': f"'{args['ID']}' ID does not exist."
            }


class Pays_unique(Resource):
    def get(self, pays):
        # read local CSV
        data = read_parquet_spark(spark, output_path_path_file)
        if pays in list(data.select(data.PAYS).toPandas()['PAYS']):
            show = data.filter(data.PAYS == pays).rdd.flatMap(lambda x: x).collect()
            return {'Pays': show}, 200  # return data dict and 200 OK
        else:
            return {
                       'message': f"'{pays}' does not exist."
                   }, 404


api.add_resource(Pays, '/data/')
api.add_resource(Pays_unique, '/data/<string:pays>')
if __name__ == '__main__':
    app.run()  # run our Flask app
