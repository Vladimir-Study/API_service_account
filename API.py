import json
from pprint import pprint
from flask import Flask
from flask_restful import Api, Resource, reqparse, abort
import psycopg2
from dotenv import load_dotenv
import os
load_dotenv()


def connections():
    conn = psycopg2.connect(
        host='rc1b-itt1uqz8cxhs0c3d.mdb.yandexcloud.net',
        port='6432',
        dbname='market_db',
        user=os.environ['DB_LOGIN'],
        password=os.environ['DB_PASSWORD'],
        target_session_attrs='read-write',
        sslmode='verify-full'
    )
    return conn


app = Flask(__name__)
api = Api(app)

param_desk = {}


class AddUser(Resource):

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument("client_id", type=int, required=True)
        parser.add_argument("service", type=int, required=True)
        parser.add_argument("service_data", type=dict, required=True)
        add_params = parser.parse_args()
        conn = connections()
        with conn:
           with conn.cursor() as cursor:
               for key, val in add_params['service_data'].items():
                   cursor.execute(f"SELECT id FROM service_attr WHERE "
                                  f"attribute_name = '{key}' and "
                                  f"service_id = {add_params['service']}")
                   service_id = cursor.fetchone()
                   print(service_id)
                   if service_id is not None:
                       cursor.execute(
                           f"INSERT INTO account_service_data "
                           f"(account_id, attribute_id, attribute_value) "
                           f"VALUES ({add_params['client_id']}, "
                           f"{service_id[0]}, {str(val)})"
                       )
                       conn.commit()
                   else:
                       abort(400, message="The argument in the service data not "
                                          "belong to the service")
        return add_params


class EditUser(Resource):

    def put(self):
        parser = reqparse.RequestParser()
        parser.add_argument("client_id", type=int, required=True)
        parser.add_argument("service", type=int, required=True)
        parser.add_argument("service_data", type=dict, required=True)
        edit_params = parser.parse_args()
        conn = connections()
        with conn:
            with conn.cursor() as cursor:
                for key, val in edit_params['service_data'].items():
                    cursor.execute(f"SELECT id FROM account_service_data "
                                    f"WHERE attribute_id = {key}")
                    attr_in_table = cursor.fetchone()
                    if attr_in_table is None:
                        abort(400, message=f"Attribute '{key}' is not table")
                    else:
                        cursor.execute(f"UPDATE account_service_data "
                                f"SET attribute_value = {val} "
                                f"WHERE attribute_id = {key}")
        return edit_params


class DeleteAccount(Resource):

    def delete(self):
        parser = reqparse.RequestParser()
        parser.add_argument("client_id", type=int, required=True)
        parser.add_argument("service", type=int, required=True)
        delete_line = parser.parse_args()
        conn = connections()
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT id FROM service_attr "
                            f"WHERE service_id = {delete_line['service']}")
                del_args = cursor.fetchall()
                for arg in del_args:
                    try:
                        cursor.execute(f"DELETE FROM account_service_data "
                                    f"WHERE account_id = {delete_line['client_id']} "
                                    f"AND attribute_id = {arg[0]}")
                        conn.commit()
                    except:
                        continue
        return { "massage": f"Client_id: {delete_line['client_id']} "
                f"for service {delete_line['service']} DELETE"}


api.add_resource(AddUser, '/account/add')
api.add_resource(EditUser, '/account/edit')
api.add_resource(DeleteAccount, '/account/delete')


if __name__ == '__main__':
    app.run(debug=True, port=5000, host="127.0.0.1")
    #pass
