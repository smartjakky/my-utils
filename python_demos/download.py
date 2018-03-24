from flask import Flask, request, send_from_directory, send_file, Response
import os
import

app = Flask(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app.config['DOWNLOAD_FOLDER'] = os.path.join(BASE_DIR, 'download')


@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/download/')
def download():
    file = request.args.get('file')
    file = os.path.join(app.config['DOWNLOAD_FOLDER'], *[path for path in file.split('/')])
    if os.path.isfile(file):
        res = Response(bytes(send_file(file)))
    else:
        res = Response(bytes(file))
    res.headers = {'Content-Type': 'application/octet-stream'}
    return res

if __name__ == '__main__':
    app.run(debug=True, port=8000)
