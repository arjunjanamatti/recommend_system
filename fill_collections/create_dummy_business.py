
import random

business_name_list = ['samsung', 'apple', 'oppo', 'toshiba', 'lenovo', 'acer', 'nokia', 'blackberry']

sample_business = {
    "_id": "609681f4fdbd561c6cea96c4",
    "loc": {
        "coordinates": [
            66.3,
            33.63
        ],
        "type": "Point"
    },
    "image": [
        "file-1620476390867.jpg"
    ],
    "isDeleted": True,
    "isActive": True,
    "name": "Tesla",
    "slug": "tesla",
    "description": "Car Manufacturer",
    "type": "Car",
    "coverImage": "file-1620476394002.jpg",
    "addedByUserId": "5fc7c1e7a3fb189c35bb8945",
    "createdAt": "2021-05-08T12:20:04.502Z",
    "updatedAt": "2021-05-08T12:20:56.895Z",
    "__v": 1
}

for i in range(900):
    new_dict = sample_business.copy()
    # 'fromUserId': '5fdf6bbcfe08e8c0191a7805'
    new_dict['_id'] = new_dict['_id'][:19]+str(799386+i)
    new_dict['title'] = random.choice(title_list)
    new_dict['loc'] = {'coordinates':cordinates_nested[i], 'type': 'Point'}
    new_dict['rating'] = random.uniform(1,10)
    new_dict['categoryId'] =  random.choice(categores_id_list)
    new_dict['fromUserId'] = random.choice(user_id_list)
    new_dict['createdAt'] = (randomDate("20-01-2021 13:30:00", "23-04-2021 04:50:34")).strftime('%d-%m-%YT%H:%M:%S')
    new_dict['updatedAt'] = (randomDate("20-01-2021 13:30:00", "23-04-2021 04:50:34")).strftime('%d-%m-%YT%H:%M:%S')
    # new_dict = sample_dict.copy()
    nested_dict.append(new_dict)