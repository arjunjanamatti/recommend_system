import requests
import os
import json
from glob import glob

# # curl -i -X POST -H "Content-Type: multipart/form-data"
# # -F "data=@test.mp3" http://mysuperserver/media/1234/upload/
# command = ['curl', "-i", '-X', 'POST', '-H', 'Content-Type: multipart/form-data', '-F',
#            'file=@C:/Users/Arjun Janamatti/PycharmProjects/try_recommend/fill_collections/arnold.mp4',
#            'http://127.0.0.1:5050/video/upload']
# video_to_audio = subprocess.Popen(command, shell=True)
# print(f'video_to_audio: {video_to_audio}')
# print(f'Result: {video_to_audio.communicate()[0]}')
# print('Upload done')

# url = 'http://127.0.0.1:5050/video/upload'
# myobj = {'file': '@C:/Users/Arjun Janamatti/PycharmProjects/try_recommend/fill_collections/arnold.mp4'}
#
# x = requests.post(url, data = myobj)
#
# print(x.status_code, x.reason)

# with open('C:/Users/Arjun Janamatti/PycharmProjects/try_recommend/fill_collections/arnold.mp4', 'r') as f:
#     r = requests.post('http://127.0.0.1:5050/video/upload', files={'C:/Users/Arjun Janamatti/PycharmProjects/try_recommend/fill_collections/arnold.mp4': f})
#
# print(r.status_code, r.reason)
import time
from pymongo import MongoClient
def GetSpeech(file_location, host):
    my_file = {'file': open(os.path.realpath(file_location), 'rb')}
    vids = requests.post(host, files=my_file)
    result_dict = json.loads(vids.text)
    return result_dict['Video speech'], result_dict['Profane words']

host = 'http://127.0.0.1:5050/text-speech'
myclient = MongoClient(host=None, port=None)
mydb = myclient['real_reviews']
coll = mydb['reviews']
review_id_list = coll.find({},{'speech': 1})
review_id_no_speech = []
for result in review_id_list:
    if 'speech' not in result:
        review_id_no_speech.append(f'{result["_id"]}')


reviews_location = 'C:/Users/Arjun Janamatti/Downloads/reviews'
reviews_list = glob('{}/*'.format(reviews_location))
print(reviews_list)

for review in reviews_list[:1]:
    first_video_list = glob('{}/*'.format(f'{review}/video'))
    print(first_video_list[-1])
    file_location = first_video_list

# file_location = 'C:/Users/Arjun Janamatti/PycharmProjects/try_recommend/fill_collections/arnold.mp4'
# host = 'http://127.0.0.1:5050/text-speech'
# def GetSpeech(file_location, host):
#     my_file = {'file': open(os.path.realpath(file_location), 'rb')}
#     vids = requests.post(host, files=my_file)
#     result_dict = json.loads(vids.text)
#     return result_dict['Video speech'], result_dict['Profane words']
#
# speech_text, profane_text = GetSpeech(file_location, host)
# print(speech_text)
# print()
# print(profane_text)

# my_file = {'file': open(os.path.realpath(file_location),'rb')}
# # You should replace 'file_to_upload' with the name server actually expect to receive
# # If you don't know what server expect to get, check browser's devconsole while uploading file manually
# start_time = time.perf_counter()
# vids = requests.post(host, files=my_file)
# print(vids.text)
# print(type(vids.text))
#
# result_dict = json.loads(vids.text)
# print(type(result_dict))
#
#
# for keys in result_dict.keys():
#     print(f'{keys}: {result_dict[keys]}')
# end_time = time.perf_counter()
# print(f'Total time: {round(end_time - start_time, 2)} seconds')
# print(vids.status_code, vids.reason)