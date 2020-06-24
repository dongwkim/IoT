// Dependencies
'use strict';
const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');

// Connection string
const url = "mongodb+srv://admin:welcomemongo@swagstore.r8dos.gcp.mongodb.net/iot?retryWrites=true&w=1";

// Create Client
async function main() {
  const client = new MongoClient(url, { useUnifiedTopology: true });

  // Await client connection
  await client.connect();
  console.log('Cnnected correctly to sever');

  const db = client.db('iot');
  var starttime = new Date('2020-06-01T00:00:00.000+09:00');
  const collection = db.collection('bucketD');
  collection.drop();
  collection.createIndex({tag_group_nm:1,date:1});

  for ( let j=0; j<6*60; j++){ // 10 Sec * 6 
    starttime.setSeconds(starttime.getSeconds()-10);
    console.log(starttime);
    for (let i=0; i< 65; i++) { // Number of Group : 196/3 = 65
      var group_nm = 'group' + i;
      for ( let k=0; k<3; k++){  // Number of Tags per Group  3
          var tag_nm = 'tag' + k ; 
          await updateDocument(db,group_nm,tag_nm,starttime);
      }
    }
  }
  await client.close();
};

// Actually run main
main().catch(console.dir);

// Upsert IoT documents
async function updateDocument(db,group_nm,tag_nm,vtime) {
  const collection = db.collection('bucketD');
  const temperature = random(30,80);
  //const date = new Date();
  //const date = vtime;

  // Create document
  const doc = {
    //deviceId: deviceId,
    tag_group_nm: group_nm,
    unit : "C",
    sample: {
      ts: vtime,
      tag_nm : tag_nm,
      tag_val: temperature},
    date: {
      year: vtime.getFullYear(),
      month: vtime.getMonth() + 1,
      day: vtime.getDate(),
      hour: vtime.getHours()
    }
  };
  //console.log(doc);
  // Upsert document (bucket/hour)
  await collection.updateOne({tag_group_nm: doc.tag_group_nm,
    "date.year": doc.date.year,
    "date.month": doc.date.month,
    "date.day": doc.date.day,
    "date.hour": doc.date.hour,
    //nsamples: {$lte: 180}
  },
    {
      $inc: {nsamples: 1},
      $set: {
        //deviceId: doc.deviceId,
        "date.year": doc.date.year,
        "date.month": doc.date.month,
        "date.day": doc.date.day,
        "date.hour": doc.date.hour,
        sensor: doc.sensor
      },
      $addToSet: {samples: doc.sample}
    },
    {upsert: true, returnNewDocument: true});

  //console.log("upserted 1");
  //sleep(100);

}

function randomInt(low, high) {
  return Math.floor(Math.random() * (high - low) + low)
};

function random(low, high) {
  return Math.random() * (high - low) + low
};

function sleep(milliseconds) {
  const date = Date.now();
  let currentDate = null;
  do {
    currentDate = Date.now();
  } while (currentDate - date < milliseconds);
};

