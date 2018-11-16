#!/bin/bash

export FLASK_APP=app/globe_app.py
export FLASK_DEBUG=0
export PYTHONPATH=`pwd`:`pwd`/app:`pwd`/app/src

function setup() {
  echo "Settingup env"
  virtualenv env

  echo "Installing requirements"
  pip install -r requirements.txt
}

source env/bin/activate
echo "Starting"
flask run