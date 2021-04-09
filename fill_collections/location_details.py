from  math import *

def validate_point(p):
    lat, lon = p
    assert -90 <= lat <= 90, "bad latitude"
    assert -180 <= lon <= 180, "bad longitude"

# original formula from  http://www.movable-type.co.uk/scripts/latlong.html
def distance_haversine(p1, p2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    Haversine
    formula:
        a = sin²(Δφ/2) + cos φ1 ⋅ cos φ2 ⋅ sin²(Δλ/2)
                        _   ____
        c = 2 ⋅ atan2( √a, √(1−a) )
        d = R ⋅ c

    where   φ is latitude, λ is longitude, R is earth’s radius (mean radius = 6,371km);
            note that angles need to be in radians to pass to trig functions!
    """
    lat1, lon1 = p1
    lat2, lon2 = p2
    for p in [p1, p2]:
        validate_point(p)

    R = 6371 # km - earths's radius

    # convert decimal degrees to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) # 2 * atan2(sqrt(a), sqrt(1-a))
    d = R * c
    return d

# [-84.62358891086079, 46.784045721242116]

# [-85.3495983399085, 45.67590014485517]

p1 = [-84.62358891086079, 46.784045721242116]
p2 = [-85.3495983399085, 45.67590014485517]
print(distance_haversine(p1[::-1], p2[::-1]))
