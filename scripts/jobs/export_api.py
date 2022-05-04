from flask import Flask, jsonify
from flask_restful import Resource, Api, reqparse
import pandas as pd
import glob

from scripts.utils.utils_file import *

app = Flask(__name__)
api = Api(app)

data_arg = reqparse.RequestParser()
data_arg.add_argument("ID", type=int, help="Enter ID")
data_arg.add_argument("C_RISK", type=str, help="Enter C_RISK")
data_arg.add_argument("C_DATE", type=str, help="Enter C_DATE")
data_arg.add_argument("C_SOURCE", type=int, help="Enter C_SOURCE")
data_arg.add_argument("DT_SOURCE", type=int, help="Enter DT_SOURCE")

# Get config user
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(ROOT_DIR, '../conf' + os.path.sep + 'config_user.yaml')
config_user = read_yaml_file(config_path)

# Get file path of output
output_path = config_user.get('output_path')
output_path_path_file = create_file_path(output_path, config_user)
file = glob.glob(output_path_path_file + os.path.sep + "*.csv")[0]

from flask import Flask
from flask_restful import Resource, Api, reqparse
import pandas as pd


app = Flask(__name__)
api = Api(app)


class pays(Resource):
    def get(self):
        data = pd.read_csv(file)  # read local CSV
        return {'data': data.to_dict()}, 200  # return data dict and 200 OK

    def post(self):

        # initialize parser
        parser = reqparse.RequestParser()
        parser.add_argument('ID', required=True, type=int)
        parser.add_argument('PAYS', required=True)
        parser.add_argument('C_RISK', required=True)

        args = parser.parse_args()  # parse arguments to dictionary

        # read our CSV
        data = pd.read_csv(file)

        # check if Id already exists
        if args['ID'] in list(data['ID']):
            # if ID already exists, return 401 unauthorized
            return {
                       'message': f"'{args['ID']}' already exists."
                   }, 409
        else:
            # otherwise, we can add the new ID record
            # create new dataframe containing new values
            new_data = pd.DataFrame({
                'ID': [args['ID']],
                'PAYS': [args['PAYS']],
                'C_RISK': [args['C_RISK']]
            })
            # add the newly provided values
            data = data.append(new_data, ignore_index=True)
            data.to_csv('C_PAYS.csv', index=False)  # save back to CSV
            return {'data': data.to_dict()}, 200  # return data with 200 OK

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


api.add_resource(pays, '/data')

if __name__ == '__main__':
    app.run()  # run our Flask app
