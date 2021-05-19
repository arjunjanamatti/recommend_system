import requests
import os
import json
from glob import glob

from pymongo import MongoClient
def GetSpeech(file_location, host):
    my_file = {'file': open(os.path.realpath(file_location), 'rb')}
    vids = requests.post(host, files=my_file)
    result_dict = json.loads(vids.text)
    return result_dict['Video speech'], result_dict['Profane words']

def UpdateKeys(id, file_location, host, speech_text, profane_text):
    # for id in id_list:
    coll.update({'_id': f'{id}'}, {'$set': {'full_speech': f'{speech_text}', 'profane_words': f'{profane_text}'}})
    pass

def GetReviewsWithNoSpeech():
    myclient = MongoClient(host=None, port=None)
    mydb = myclient['real_reviews']
    coll = mydb['reviews']
    review_id_list = coll.find({}, {'full_speech': 1})
    review_id_no_speech = []
    [review_id_no_speech.append(f'{result["_id"]}') for result in review_id_list
     if 'full_speech' not in result]
    return review_id_no_speech

def GetUpdatedReviewId(reviews_location):
    reviews_list = glob('{}/*'.format(reviews_location))
    update_review_list = []

    for review in reviews_list:
        review_id = review.split('\\')[-1]
        if review_id in review_id_no_speech:
            update_review_list.append(review)

    return update_review_list
    pass

host = 'http://127.0.0.1:5050/text-speech'
myclient = MongoClient(host=None, port=None)
mydb = myclient['real_reviews']
coll = mydb['reviews']
review_id_list = coll.find({},{'full_speech': 1})
review_id_no_speech = []
for result in review_id_list:
    if 'full_speech' not in result:
        review_id_no_speech.append(f'{result["_id"]}')


reviews_location = 'C:/Users/Arjun Janamatti/Downloads/reviews'
reviews_list = glob('{}/*'.format(reviews_location))
# print(reviews_list)
# print(review_id_no_speech)

update_review_list = []

for review in reviews_list:
    review_id = review.split('\\')[-1]
    # print(review_id)
    if review_id in review_id_no_speech:
        update_review_list.append(review)

# print(update_review_list)

for review in update_review_list:
    first_video_list = glob('{}/*'.format(f'{review}/video'))
    print(first_video_list[-1])
    file_location = first_video_list[-1]
    speech_text, profane_text = GetSpeech(file_location, host)
    print(speech_text)
    print(type(speech_text))
    print(profane_text)
    print(type(profane_text))
    review_id = review.split('\\')[-1]
    UpdateKeys(review_id, file_location, host, speech_text, profane_text)
    print(f'update on Review_id: {review_id} completed !!!')


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