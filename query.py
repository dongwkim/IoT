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
def query_agg(coll,groupnm,tagnm,stime,etime):
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
            }, {
                '$unwind': {
                    'path': '$tag'
                }
            }, {
                '$bucketAuto': {
                    'groupBy': '$tag.ts', 
                    'buckets': 2880, 
                    'output': {
                        'tag_group_nm': {
                            '$min': '$tag_group_nm'
                        }, 
                        'tag_nm': {
                            '$min': '$tag_nm'
                        }, 
                        'ts': {
                            '$first': '$tag.ts'
                        }, 
                        'first': {
                            '$first': '$tag.val'
                        }, 
                        'avg': {
                            '$avg': '$tag.val'
                        }
                    }
                }
            }, {
                '$group': {
                    '_id': '$tag_nm', 
                    'trend': {
                        '$push': {
                            'ts': '$ts', 
                            'val': '$first'
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
    print("tag: {},interval {}".format(result[0]['_id'],result[0]['trend'][1]['ts'] -  result[0]['trend'][0]['ts']))
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
                    'buckets': 2880, 
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
            # {
            #     '$sort':{
            #         'trend.ts':1
            #     }
            # }
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

def query_agg_facet(coll,groupnm,tagnm):
    pipeline = [
    {
        '$match': {
            'tag_group_nm': groupnm, 
            'first': {
                '$gte': datetime(2020, 5, 1, 0, 0, 0, tzinfo=timezone.utc)
            }
        }
    }, {
        '$unwind': {
            'path': '$tag'
        }
    }, {
        '$facet': {
            'tag0': [
                {
                    '$match': {
                        'tag_nm': 'tag0'
                    }
                }, {
                    '$bucketAuto': {
                        'groupBy': '$tag.ts', 
                        'buckets': 2000, 
                        'output': {
                            'tag_group_nm': {
                                '$min': '$tag_group_nm'
                            }, 
                            'tag_nm': {
                                '$min': '$tag_nm'
                            }, 
                            'ts': {
                                '$first': '$tag.ts'
                            }, 
                            'first': {
                                '$first': '$tag.val'
                            }, 
                            # 'avg': {
                            #     '$avg': '$tag.val'
                            # }
                        }
                    }
                }
            ], 
            'tag1': [
                {
                    '$match': {
                        'tag_nm': 'tag1'
                    }
                }, {
                    '$bucketAuto': {
                        'groupBy': '$tag.ts', 
                        'buckets': 2000, 
                        'output': {
                            'tag_group_nm': {
                                '$min': '$tag_group_nm'
                            }, 
                            'tag_nm': {
                                '$min': '$tag_nm'
                            }, 
                            'ts': {
                                '$first': '$tag.ts'
                            }, 
                            'first': {
                                '$first': '$tag.val'
                            }, 
                            # 'avg': {
                            #     '$avg': '$tag.val'
                            # }
                        }
                    }
                }
            ], 
            'tag2': [
                {
                    '$match': {
                        'tag_nm': 'tag2'
                    }
                }, {
                    '$bucketAuto': {
                        'groupBy': '$tag.ts', 
                        'buckets': 2000, 
                        'output': {
                            'tag_group_nm': {
                                '$min': '$tag_group_nm'
                            }, 
                            'tag_nm': {
                                '$min': '$tag_nm'
                            }, 
                            'ts': {
                                '$first': '$tag.ts'
                            }, 
                            'first': {
                                '$first': '$tag.val'
                            }, 
                            # 'avg': {
                            #     '$avg': '$tag.val'
                            # }
                        }
                    }
                }
            ]
        }
    }
    ] 
    start = time.time()
    #print(pipeline)
    result = list(coll.aggregate(pipeline))
    end = time.time()
    elapse = end - start
    # pprint.pprint(result)
    qlen = len(result[0]['tag0'])
    print("tag: {},interval {}".format('tag0',result[0]['tag0'][1]['ts'] -  result[0]['tag0'][0]['ts']))
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
    startTime =datetime(2020, 2,1, 0, 0, 0, tzinfo=timezone.utc)

    endTimeEpoch = endTime.timestamp()
    startnTimeEpoch = startTime.timestamp()

    timediff = (endTime-startTime).total_seconds()
    modSec = ((timediff/86400)+1) * 30
    #for i in range(64):
    while True:
        #group = 'group' + str(i)
        cnt += 1
        group = 'group' + str(randint(0,63))
        # result.append(query_agg_facet(coll,group,'facet'))
        for j in range(3):
            tag = 'tag' + str(j)
            #result.append(query(coll,groupnm=group,tagnm=tag))
            if timediff < 3600 * 24 * 14 :
                result.append(query_agg(coll,groupnm=group,tagnm=tag,stime=startTime,etime=endTime))
            else:
                result.append(query_agg_10min(coll,groupnm=group,tagnm=tag,stime=startTime,etime=endTime))
        elapsed = sum([pair[0] for pair in result])
        total_elapsed += elapsed
        avg_elapsed = total_elapsed/cnt
        print("{:10s} : elapsed :{:5f} : avg elapsed :{:5f}, detail{}".format(group,elapsed,avg_elapsed,result))
        result = []
        time.sleep(0)
    """
    while True:
        group = 'group' + str(randint(0,10))
        tag = 'tag' + str(randint(0,3))
        result = query(coll,groupnm=group,tagnm=tag)
        print("{},{} : elapsed :{}".format(group,tag,result))
    """


if __name__ == '__main__':
    main()
