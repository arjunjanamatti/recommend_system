from pymongo import MongoClient


myclient = MongoClient(host=None, port=None)
mydb = myclient['real_reviews']
coll = mydb['reviews']
# mongo_db = myclient.real_reviews
# allkeyvalues = mongo_db.reviews.find({},{'testing_new_key': 1})
allkeyvalues = coll.find({},{'testing_new_key': 1})
for result in allkeyvalues:
        if 'testing_new_key' in result.keys():
                print(result)
                print(result['_id'])

# # working to find all _id in the table
# list_id = coll.find().distinct('_id')
# print(list_id)


# # working update below
# coll.update({'_id': '60559e8398141c145f30584b'}, {'$set': {'testing_new_key': 'testing new value'}})