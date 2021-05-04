import os
from flask import Flask, request, redirect, url_for
from werkzeug.utils import secure_filename

# UPLOAD_FOLDER = 'saved_videos'
app = Flask(__name__)

def makedir(file):
    try:
        os.mkdir(file)

    except Exception as e:
        print(f'Execption in making directory: {e}')

    pass

@app.route('/savefile', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        file = request.files['file']
        print(file.filename)
        foldername = file.filename
        makedir(foldername.split('.')[0])
        filename = secure_filename(file.filename)
        file.save(foldername, filename)
        return {'Result': "Uploaded file"}
    pass

if __name__ == '__main__':
    app.run(debug=True)