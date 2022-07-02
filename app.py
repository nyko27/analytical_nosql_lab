from flask import Flask, jsonify, request
from data_transference_handler import DataTransferenceHandler

app = Flask(__name__)


@app.post('/execute')
def send_data():
    dth = DataTransferenceHandler(**request.json)

    return jsonify(dth.write_data())


if __name__ == '__main__':
    app.run(host='0.0.0.0')
