#!/usr/bin/env node

var program = require('commander');
var async = require('async');
var request = require('request');
var url = require('url');

program
    .version('0.0.8')
    .usage('[options] bucketName')
    .option('-H, --host [host]','specify the host (default: localhost)')
    .option('-p, --port [port]','specify the post (default: 8098)')
    .option('-f, --file [FileName]','specify the file name (default: [bucket].json)')
    .option('-i, --import','import mode (instead of reading from bucket entries will be written to bucket)')
    .option('-c, --concurrency [concurrency]','specify the concurrency (default: 20)')
    .option('-m, --meta [meta]', 'import with meta (default: False)')
    .option('-P, --pretty [pretty]', 'pretty stringify of json (default: False)')
    .option('--delete', 'delete the keys as they are exported (DANGER: possible data loss)')
    .parse(process.argv);
if(!program.args.length) {
    program.help();
}
var bucket = program.args;
program.host = program.host || 'localhost';
program.port = program.port || '8098';
program.file = program.file || bucket+'.json';
program.concurrency = !isNaN(program.concurrency) ? parseInt(program.concurrency, 10) : 20;
program.meta = (program.meta==='true')  || false;
program.pretty = (program.pretty==='true') || false;
var count = 0;
var openWrites = 0;
var db = require("riak-js").getClient({host: program.host, port: program.port});
var fs = require('fs');
var deleteKeys = !program.import && !!program.delete;
var riakUrl = 'http://' + program.host + ":" + program.port + '/riak';

if (program.import) {
  importToBucket();
} else {
  if (program.delete) {
    console.log('WARNING: keys will be deleted as they are exported');
  }

  exportFromBucket();
}

function importToBucket() {
  if (!fs.existsSync(program.file)) {
    throw new Error('the import file does not exist');
  }
  fs.readFile(program.file, 'utf8', function (err,data) {
    if (err) {
      return console.log(err);
    }
    var entries = JSON.parse(data);
    async.eachLimit(entries, program.concurrency, function(entry, cb) {
      console.log('inserting entry with key %j', entry.key);
      var meta = {index: entry.indexes};
      if(entry.meta){
        meta = entry.meta;
      }
      db.save(bucket, entry.key, entry.data, meta, function(err) {
        if (err) {
          return cb(err);
        }
        cb(null);
      });
    }, function(err) {
      if (err) {
        return console.log(err);
      }
      return console.log('%j entries inserted into bucket %j', entries.length, bucket);
    });
  });
}

var receivedAll = false;
var q = async.queue(processKey, program.concurrency);
q.drain = end;

function exportFromBucket() {
  if (fs.existsSync(program.file)) {
    throw new Error('the output file already exists');
  }
  console.log('fetching bucket '+bucket+' from '+program.host+':'+program.port);
  db.keys(bucket,{keys:'stream'}, function (err) {
    if (err) {
      console.log('failed to fetch keys');
      console.log(err);
    }
  }).on('keys', handleKeys).on('end', function() {
    console.log('received all keys');
    receivedAll = true;
  }).start();
}

function end() {
  if (!receivedAll) {
    return;
  }
  if (count<=0) {
    console.log('nothing exported');
  } else {
    console.log('finished export of '+count+' keys to '+program.file);
  }
}

function handleKeys(keys) {
  count+=keys.length;
  for (var i=0;i<keys.length;i++) {
    var key = keys[i];
    openWrites++;
    q.push(key, function() {
      openWrites--;
    });
  }
  console.log('queue size: ' + q.length());
}

var isValidJSON = function(data){
  try{
    return JSON.parse(data);
  }
  catch(err){
    return false
  }
};

function processKey(key, cb) {
  console.log('exporting key ' + key);
  var keyUrl = [riakUrl, bucket, key].join('/');
  request(keyUrl, function(err, response, body){
    if(err || response.statusCode !== 200){
      console.log('ERROR', err, response && response.statusCode, keyUrl);
      return cb();
    }

    var out = {
      key: [bucket, key],
      headers: response.headers
    };

    var data = isValidJSON(body);

    if(data){
      out.data = data;
    }
    else{
      out.data = new Buffer( body, 'binary' ).toString('base64');
    }

    var options = [out];
    if(program.pretty){
      options = options.concat([null, '\t']);
    }
    fs.appendFileSync(program.file, JSON.stringify.apply(this, options) + '\n');

    return cb();
  });
}
