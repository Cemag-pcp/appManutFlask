from flask import Blueprint, Flask
from routes import routes_bp
from login import login_bp
import psycopg2 #pip install psycopg2 
import psycopg2.extras
import os

def create_app():

    app = Flask(__name__)
    app.config['UPLOAD_FOLDER'] = r'C:\Users\pcp2\projetoManutencao\appManutFlask-3\UPLOAD_FOLDER'
    app.register_blueprint(routes_bp)
    app.register_blueprint(login_bp)
    app.secret_key = "manutencaoprojeto"

    return app

# DB_HOST = "localhost"
DB_HOST = "database-1.cdcogkfzajf0.us-east-1.rds.amazonaws.com"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASS = "15512332"
 
conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST)

# @app.route('/teste')
# def index():
#     return 'Página Inicial'

# if __name__ == '__main__':
#     app.run()