from pymongo import MongoClient

def TrialFunctions():
        return 10 + 20

def UpdateKeys(id_list):
        for id in id_list[:int(0.5*len(id_list))]:
                coll.update({'_id': f'{id}'}, {'$set': {'testing_new_key': f'{str(TrialFunctions())}'}})
        pass

myclient = MongoClient(host=None, port=None)
mydb = myclient['real_reviews']
coll = mydb['reviews']
# mongo_db = myclient.real_reviews
# allkeyvalues = mongo_db.reviews.find({},{'testing_new_key': 1})
allkeyvalues = coll.find({},{'testing_new_key': 1})
non_available_id = []
for result in allkeyvalues:
        if 'testing_new_key' not in result.keys():
                print(result)
                non_available_id.append(result['_id'])

UpdateKeys(non_available_id)


# # working to find all _id in the table
# list_id = coll.find().distinct('_id')
# print(list_id)


# # working update below
# coll.update({'_id': '60559e8398141c145f30584b'}, {'$set': {'testing_new_key': 'testing new value'}})