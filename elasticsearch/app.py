import sys
import os
from flask import Flask

app = Flask(__name__)

app = Flask(__name__, instance_path=os.path.join(os.path.abspath(os.path.dirname(__file__)), 'instance'), instance_relative_config=True)
app.config.from_pyfile('app.cfg')

@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route('/translate/<uuid>', methods=['GET'])
def translate():
    return 'OK'