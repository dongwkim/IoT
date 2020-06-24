from threading import Thread, Lock
from pymongo import MongoClient
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError
from time import strftime,localtime
from random import randint

import logging
import datetime
import time
import argparse
import random
import multiprocessing
from datetime_truncate import truncate

def temperature(low, high):
    #temp = randint(low,high)
    temp = random.random() * (high - low) + low
    return temp

def update_type1(coll,doc,bulkopt,request):
    coll.update_one(
                     {"tag_group_nm": doc["group_nm"], "date": doc["ts"].strftime("%Y-%m-%d %H")},
                     {
                         "$min" : {"first": doc["ts"]},
                         "$max" : {"last": doc["ts"]},
                         "$inc" : {"nsamples" : 1},
                         "$addToSet": {"tag": {"ts":doc["ts"],"val":doc["temp"]}}
                     },
                     upsert=True
                )
def update_type2(coll,doc,bulkopt,request):
    coll.update_one(
                     {"tag_group_nm": doc["group_nm"], "tag_nm":doc["tag_nm"], "date": truncate(doc["ts"],'10_minute').strftime("%Y-%m-%d %H%M")},
                     {
                         "$min" : {"first": doc["ts"]},
                         "$max" : {"last": doc["ts"]},
                         "$inc" : {"nsamples" : 1, "total":doc["temp"]},
                         "$set"  : {"val": doc["temp"]},
                         "$addToSet": {"tag": {"ts":doc["ts"],"val":doc["temp"]}}
                         #"$addToSet": {"tag": {doc["ts"]:doc["temp"]}}
                     },
                     upsert=True
                )

def update_type3(coll,doc,bulkopt,request):
    coll.update_one(
                     {"tag_group_nm": doc["group_nm"], "date": doc["ts"].strftime("%Y-%m-%d %H")},
                     {
                         "$min" : {"first": doc["ts"]},
                         "$max" : {"last": doc["ts"]},
                         "$inc" : {"nsamples" : 1},
                         "$addToSet": {"tag": {"ts":doc["ts"],"val":doc["temp"]}}
                         #"$addToSet": {"tag": {doc["ts"]:doc["temp"]}}
                     },
                     upsert=True
                )
'''
def update_typeXX(coll,doc):
    doc = { 
                    "tag_group_nm": doc["group_nm"], 
                    "unit" : "c", 
                    "tag": [{"tag_nm" : doc["tag_nm"], "trend": [{str(doc["start_time"]):temp}]} ], 
                    "date": doc["start_time"].strftime("%Y-%m-%d %H"),
                    "first" : doc["start_time"]
                    #"date": { "year": start_time.year, "month": start_time.month, "day": start_time.day, "hour": start_time.hour},
           }
    result = coll.find_one({"tag_group_nm": doc["tag_group_nm"], "date": doc["first"].strftime("%Y-%m-%d %H")},{"_id"})
    #print(result)
    if result is not None:
        coll.update_one(
                         {"tag_group_nm": doc["tag_group_nm"], "date": doc["first"].strftime("%Y-%m-%d %H")},
                         {
                             "$min" : {"first": doc["first"]},
                             "$max" : {"last": doc["first"]},
                             "$inc" : {"nsamples" : 1},
                             "$addToSet": {"tag.$[elem].trend": doc["tag"][0]["trend"][0]}
                         },
                         upsert=False,
                         array_filters=[{"elem.tag_nm":doc["tag"][0]["tag_nm"]}]
                    )
    else:
        coll.insert_one(doc)
'''
def worker(uri,dbname,colname,group):
    logging.debug('Run')
    
    client = MongoClient(uri)
    db = client[dbname]
    coll=db.get_collection(colname)
    
    total_count=0

    request=[]
    
    start_time=datetime.datetime(2020,2,29,0,0,0)
    # 6*60*24 : 1day
    # 6*60*24*30 : 1Month
    for i in range(6*60*24*29):
        # reduce time here
        start_time = start_time - datetime.timedelta(seconds=10)
        if i%(6*60*24) == 0: 
            print("group# {}: {} days done".format(group,i/(360*24)))
        for j in range(1):
            #group_nm = 'group' + str(j)
            # convert Thread# to group name
            group_nm = 'group' + str(group)
            for k in range(3):
                tag_nm = 'tag' + str(k)
                temp = temperature(30,50)
                data = { "ts": start_time,"group_nm": group_nm, "tag_nm":tag_nm, "temp": temp}
                #type1: put every tags in tag array
                #update_type1(coll,data,(i*3)+k,request)
                #update_type1(coll,data)
                #type2: grouping tags in tag array, TODO : Only can group single tag
                #update_type2X(coll,data,(i*3)+k,request)
                update_type2(coll,data,(i*3)+k,request)
    # Finalyze commit
    #coll.bulk_write(request)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', type=str, default="iot", help="MongoDB Database Name")
    parser.add_argument('-c', type=str, default="test_coll", help="MongoDB Database Collection")
    parser.add_argument('-t', type=int, default=1, help="Thread Count")
    parser.add_argument('-y', type=int, default=1, help="Delay")
    parser.add_argument('-w', type=int, default=10000, help="Write Count")
    parser.add_argument('-uri', type=str, default="mongodb+srv://admin:welcomemongo@trend.r8dos.mongodb.net/iot?retryWrites=true&w=1",help="MongoDB uri")


    args = parser.parse_args()
    uri = args.uri
    dbname = args.d
    colname = args.c
    delay = args.y
    tc = args.t # Thread Count
    target_count = args.w

    #client = MongoClient(uri)

    """
    ts = [Thread(name=f'Thread{i+1:02d}',target=worker, args=(uri,dbname,colname,i)) for i in range(tc)]
    for t in ts:
        t.start()

    time.sleep(1)

    for t in ts:
        t.join()
    """

    jobs = []
    for i in range(tc):
        p = multiprocessing.Process(target=worker, args=(uri,dbname,colname,i,))
        jobs.append(p)
        p.start()



if __name__ == '__main__':
    main()
