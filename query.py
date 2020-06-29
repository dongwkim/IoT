from pymongo import MongoClient
import pymongo
import argparse
from datetime import datetime,timezone
import pprint
import time
from random import randint
from pymongo import ReadPreference

"""
    단순 조회 - 전체 데이터 리턴 
"""
def query(coll,groupnm,tagnm):
    query = {
                    'tag_group_nm': groupnm, 
                    'tag_nm': tagnm, 
                    'first': {
                        '$gte': datetime(2020, 5, 1, 0, 0, 0, tzinfo=timezone.utc)
                    }
            }
    start = time.time()
    result = list(coll.find(query))
    end = time.time()
    elapse = end - start
    #pprint.pprint(result)
    #qlen = len(result[2])
    #print(result)
    qlen=''
    return elapse,qlen

"""
    Aggregation - 14일 이내 데이터 조회시
"""
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
                    'buckets': bsz, 
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
                        },
                        'unit' :{
                            '$first':'$unit'
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
                    },
                    'unit':{
                        '$first': '$unit'
                    }
                }
            },{
                '$project':{
                    '_id':0,
                    "tag_nm":'$_id',
                    'trend':1,
                    'unit' :1
                }
            },
            #  {
            #     '$sort':{
            #         'trend.ts':1
            #     }
            # }
        ]
    start = time.time()
    result = list(coll.aggregate(pipeline))
    end = time.time()
    elapse = end - start
    qlen = len(result[0]['trend'])
    print("tag: {},interval {}".format(result[0]['tag_nm'],result[0]['trend'][1]['ts'] -  result[0]['trend'][0]['ts']))
    # 소요시간, 반환되는 어레이 길이, 결과값 
    return elapse,qlen,result

"""
    Aggregation - 14일 이상 데이터 조회시
    Inner Array의 값을 버케팅 하지 않고 Root Document의 Computed 값을 사용
"""
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
                    'buckets': bsz, 
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
                        },
                        'unit' :{
                            '$first':'$unit'
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
                    },
                    'unit':{
                        '$first': '$unit'
                         
                    }
                }
            },{
                '$project':{
                    '_id':0,
                    "tag_nm":'$_id',
                    'trend':1,
                    'unit':1
                }
            }
            # {
            #     '$sort':{
            #         'trend.ts':1
            #     }
            # }
        ]
    start = time.time()
    result = list(coll.aggregate(pipeline))
    end = time.time()
    elapse = end - start
    qlen = len(result[0]['trend'])
    print("tag: {},interval {}".format(result[0]['tag_nm'],(result[0]['trend'][1]['ts'] -  result[0]['trend'][0]['ts']).total_seconds()))
    # 소요시간, 반환되는 어레이 길이, 결과값 
    return elapse,qlen,result

'''
def query_agg_facet(coll,groupnm):
    pipeline = [
    {
                '$match': {
                    'tag_group_nm': groupnm, 
                    # 'tag_nm': tagnm, 
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
        '$facet': {
            'tag0': [
                {
                    '$match': {
                        'tag_nm': 'tag0'
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
'''

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', type=str, default="iot", help="MongoDB Database Name")
    parser.add_argument('-c', type=str, default="type2", help="MongoDB Database Collection")
    parser.add_argument('-uri', type=str, default="mongodb+srv://admin:welcomemongo@trend.r8dos.mongodb.net/iot?readPreference=secondary",help="MongoDB uri")
    parser.add_argument('-repl', type=str, default="atlas-jay9vh-shard-0",help="ReplicaSet")
    args = parser.parse_args()
    uri = args.uri
    dbname = args.d
    colname = args.c
    replset = args.repl
    
    client = MongoClient(uri,replicaset=replset,readPreference='secondaryPreferred')
    # client = MongoClient(uri,replicaset='atlas-jay9vh-shard-0',readPreference='primary')
    db = client[dbname]
    coll=db.get_collection(colname)

    result =[]
    total_elapsed =0
    output =[]
    cnt =0
    
    

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
            if timediff < 3600 * 24 * 17 :
                result.append(query_agg(coll,groupnm=group,tagnm=tag,stime=startTime,etime=endTime))
            else:
                result.append(query_agg_10min(coll,groupnm=group,tagnm=tag,stime=startTime,etime=endTime))
        elapsed = sum([pair[0] for pair in result])

        # 쿼리 검증
        if showoutput is True:
            output.append([pair[2][0] for pair in result])
            print(output[0])
        total_elapsed += elapsed
        avg_elapsed = total_elapsed/cnt
        print("{:10s} : elapsed :{:5f} : avg elapsed :{:5f}, detail{}".format(group,elapsed,avg_elapsed,[pair[0:2] for pair in result]))
        result = []
        output=[]
        time.sleep(1)
    """
    while True:
        group = 'group' + str(randint(0,10))
        tag = 'tag' + str(randint(0,3))
        result = query(coll,groupnm=group,tagnm=tag)
        print("{},{} : elapsed :{}".format(group,tag,result))
    """


if __name__ == '__main__':

    # 쿼리 결과 조회여부
    showoutput = False
    # bucketSize
    bsz=2880

    startTime =datetime(2020, 2,1, 0, 0, 0, tzinfo=timezone.utc)
    endTime =datetime(2020, 5, 31, 23, 59, 0, tzinfo=timezone.utc)

    main()
