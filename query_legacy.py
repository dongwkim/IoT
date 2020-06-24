from pymongo import MongoClient
import pymongo
import argparse
from datetime import datetime,timezone
import pprint
import time
from random import randint
from pymongo import ReadPreference

def query(coll,groupnm,tagnm):
    query = {
                    'tag_group_nm': groupnm, 
                    'tag_nm': tagnm, 
                    'first': {
                        '$gte': datetime(2020, 5, 1, 0, 0, 0, tzinfo=timezone.utc)
                    }
            }
    start = time.time()
    #print(pipeline)
    result = list(coll.find(query))
    end = time.time()
    elapse = end - start
    #pprint.pprint(result)
    #qlen = len(result[2])
    #print(result)
    qlen=''
    return elapse,qlen
def query_agg(coll,groupnm,stime,etime,mod):
    pipeline = [
    {
        '$match': {
            'tag_group_nm': groupnm, 
            'first': {
                '$gte': stime
            }, 
            'last': {
                '$lt': etime
            }
        }
    }, {
        '$unwind': {
            'path': '$tag'
        }
    }, {
        '$addFields': {
            'timestamp': {
                '$subtract': [
                    '$tag.ts', datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
                ]
            }
        }
    }, {
        '$addFields': {
            'timestamp': {
                '$mod': [
                    '$timestamp', mod 
                ]
            }
        }
    }, {
        '$match': {
            'timestamp': 0
        }
    }, {
        '$group': {
            '_id': '$tag_nm', 
            'trend': {
                '$push': '$tag'
            }
        }
    }
]
    start = time.time()
    #print(pipeline)
    result = list(coll.aggregate(pipeline))
    end = time.time()
    elapse = end - start
    # pprint.pprint(result)
    qlen = []
    interval =[]

    for i in range(len(result)):
        qlen.append(len(result[i]['trend']))
        interval.append((result[i]['_id'],(result[i]['trend'][1]['ts'] -  result[i]['trend'][0]['ts']).total_seconds()))
    print("interval {}".format(interval))
    return elapse,qlen

def query_agg_10min(coll,groupnm,tagnm,stime,etime):
    pipeline = [
            {
                '$match': {
                    'tag_group_nm': groupnm, 
                    'tag_nm': tagnm, 
                    'first': {
                        '$gte': stime
                    },
                    'last': {
                        '$lt': etime
                    }
                }
            },{
                '$bucketAuto': {
                    'groupBy': '$last', 
                    'buckets': 2000, 
                    'output': {
                        'tag_group_nm': {
                            '$min': '$tag_group_nm'
                        }, 
                        'tag_nm': {
                            '$min': '$tag_nm'
                        }, 
                        'ts': {
                            '$first': '$last'
                        }, 
                        'val': {
                            '$first': '$val'
                        }
                    }
                }
            }, {
                '$group': {
                    '_id': '$tag_nm', 
                    'trend': {
                        '$push': {
                            'ts': '$ts', 
                            'val': '$val'
                        }
                    }
                }
            },
            {
                '$sort':{
                    'trend.ts':1
                }
            }
        ]
    start = time.time()
    #print(pipeline)
    result = list(coll.aggregate(pipeline))
    end = time.time()
    elapse = end - start
    # pprint.pprint(result)
    qlen = len(result[0]['trend'])
    print("tag: {},interval {}".format(result[0]['_id'],(result[0]['trend'][1]['ts'] -  result[0]['trend'][0]['ts']).total_seconds()))
    return elapse,qlen

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', type=str, default="iot", help="MongoDB Database Name")
    parser.add_argument('-c', type=str, default="type2", help="MongoDB Database Collection")
    parser.add_argument('-t', type=int, default=1, help="Thread Count")
    parser.add_argument('-y', type=int, default=1, help="Delay")
    parser.add_argument('-w', type=int, default=10000, help="Write Count")
    parser.add_argument('-uri', type=str, default="mongodb+srv://admin:welcomemongo@trend.r8dos.mongodb.net/iot?readPreference=secondary",help="MongoDB uri")
    args = parser.parse_args()
    uri = args.uri
    dbname = args.d
    colname = args.c
    delay = args.y
    tc = args.t # Thread Count
    target_count = args.w
    
    client = MongoClient(uri,replicaset='atlas-jay9vh-shard-0',readPreference='secondaryPreferred')
    # client = MongoClient(uri,replicaset='atlas-jay9vh-shard-0',readPreference='primary')
    db = client[dbname]
    coll=db.get_collection(colname)

    result =[]
    total_elapsed =0
    cnt =0
    endTime =datetime(2020, 5, 31, 23, 59, 0, tzinfo=timezone.utc)
    startTime =datetime(2020, 5, 12, 0, 0, 0, tzinfo=timezone.utc)

    endTimeEpoch = endTime.timestamp()
    startnTimeEpoch = startTime.timestamp()

    timediff = (endTime-startTime).total_seconds()
    modSec = int((timediff/86400)+1) * 30 * 1000
    print(modSec)
    #for i in range(64):
    while True:
        cnt += 1
        #group = 'group' + str(i)
        group = 'group' + str(randint(0,63))
        # result.append(query_agg_facet(coll,group,'facet'))
        result.append(query_agg(coll,groupnm=group,stime=startTime,etime=endTime,mod=modSec))
        elapsed = sum([pair[0] for pair in result])
        total_elapsed += elapsed
        avg_elapsed = total_elapsed/cnt
        print("{:10s} : elapsed :{:5f} : avg :{:5f}, detail{}".format(group,elapsed,avg_elapsed,result))
        result = []
        time.sleep(1)
    """
    while True:
        group = 'group' + str(randint(0,10))
        tag = 'tag' + str(randint(0,3))
        result = query(coll,groupnm=group,tagnm=tag)
        print("{},{} : elapsed :{}".format(group,tag,result))
    """


if __name__ == '__main__':
    main()
