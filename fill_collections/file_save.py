import os
from flask import Flask, request, redirect, url_for
from werkzeug.utils import secure_filename

UPLOAD_FOLDER = '/'
app = Flask(__name__)

@app.route('/video/upload', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        file = request.files['file']
        filename = secure_filename(file.filename)
        file.save(UPLOAD_FOLDER, filename)
    pass

if __name__ == '__main__':
    app.run(debug=True)