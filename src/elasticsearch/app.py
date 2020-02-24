import sys
import os
from flask import Flask
from main import Main
import threading

app = Flask(__name__)

app = Flask(__name__, instance_path=os.path.join(os.path.abspath(os.path.dirname(__file__)), 'instance'), instance_relative_config=True)
app.config.from_pyfile('app.cfg')

@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route('/reindex/<uuid>', methods=['PUT'])
def reindex(uuid):
    try:
        main = Main('entities')
        t1 = threading.Thread(target=main.reindex, args=[uuid])
        t1.start()
    except Exception as e:
        print(e)
    return 'OK'