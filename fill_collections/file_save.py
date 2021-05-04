import os
from flask import Flask, request, redirect, url_for, jsonify
from werkzeug.utils import secure_filename

# UPLOAD_FOLDER = 'saved_videos'
app = Flask(__name__)

def makedir(file):
    try:
        os.mkdir(file)

    except Exception as e:
        print(f'Execption in making directory: {e}')

    pass

@app.route('/savefile', methods=['POST'])
def upload_file():
    # if request.method == 'POST':
    file = request.files['file']
    filename = secure_filename(file.filename)
    app.config['UPLOAD_FOLDER'] = '.'
    file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
    resp = jsonify({'message': 'File successfully uploaded'})
    return resp



if __name__ == '__main__':
    app.run(debug=True)