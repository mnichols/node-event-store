var net = require('net')
  , util = require('util')
  , Stream = require('stream').Stream
  , es = require('event-stream')
  , formatString1 = '*%d\r\n'
  , formatString2 = '$%d\r\n%s\r\n'
  , replace1 = /^\$[0-9]+/
  , replace2 = /^\*[0-9]+|^\:|^\+|^\$|^\r\n$/

function Redis (port, host, db, auth) { 
  var cfg = {
    port: port
    ,host: host
    ,db: db
  }
  this.port = port || 6379
  this.host = host || 'localhost'
  this.db = String(db || 0)
  this.pool = require('./connection-pool')(cfg)
  return this
}

// expose event-stream for convenience
Redis.es = es


Redis.prototype.createConnection = function () {
  
  return net.createConnection(this.port, this.host)
}

Redis.prototype.stream = function (cmd, key, curry /* moar? */) {
  var curry = Array.prototype.slice.call(arguments)
    , clip = 1
    , client = this
    , stream

  var xform = new Stream() 
  xform.writable = true
  xform.readable = true
  xform.write = function (args) {
    //accept arrays as data for `write`
    var elems = concat([], stream.curry)
    elems = concat(elems, args)
    var parsed = Redis.parse(elems)
    client.pool.acquire(function (err, conn) {
        stream.selectDb(conn)
        xform.once('end', function () {
            client.pool.release(conn)
            stream.db = null
        })
        conn.on('data', function (raw) {
            console.log('raw',raw.toString())
            xform.emit('data', raw)
        })
        conn.write(parsed)
    })
  }
  xform.end = function () {
    if(xform.ended) return
    xform.ended = true
    xform.readable = xform.writable = false
    xform.emit('end')
    xform.emit('close')
  }
  var reply = es.pipeline(es.split('\r\n'), es.map(replyParser))
  stream = es.pipeline(xform, reply)
  stream.selectDb = function (conn) {
    if(stream.db === client.db) return
    stream.db = client.db
    conn.write(Redis.parse(['select', client.db]))
  }
  stream.curry = curry

//  var curry = Array.prototype.slice.call(arguments)
//    , clip = 1
//    , _redis = this.createConnection()
//    , stream = es.pipe(
//        es.pipe(
//          es.map(function (data, fn) {
//              //accept arrays as data for `write`
//              var elems = concat([], stream.curry)
//              elems = concat(elems, data)
//              return Redis.parse(elems, fn)
//            }), 
//          _redis
//        ), 
//        es.pipe(
//          es.split('\r\n'), 
//          es.map(replyParser)
//        )
//      )
//    ;
//  stream.curry = curry 
//  stream.redis = _redis
//  stream.redis.write(Redis.parse([ 'select', this.db ]))
  return stream

  //presumably faster than Array.concat
  function concat (target, data) {
    target = target || []
    if(Object.prototype.toString.call(data)!=='[object Array]') {
        data = [data]
    }
    Array.prototype.push.apply(target, data)
    return target
  }

  function replyParser (data, fn) {
    if (Redis.debug_mode) console.log('replyParser', data+'')
    //first intercept special multi-bulk replies
    if (data.indexOf('*0')===0) {
        //multi-bulk replies should return empty
        //"If the specified key does not exist, the key is considered 
        //to hold an empty list and the value 0 
        //is sent as multi bulk count."
        return fn(null, '')
    }
    if (data.indexOf('*-1') === 0) {
        //multi-bulk replies report error condition as nil
        //"A client library API SHOULD return a nil object and 
        //not an empty list when this happens. 
        //This is necessary to distinguish between an empty list and an 
        //error condition (for instance the timeout condition of the BLPOP command)."
        return fn(null, null)
    }
    var str = (data+'').replace(replace1, '').replace(replace2, '')
    if (!str.length) return fn()
    else if (clip) {
      clip--
      return fn()
    }
    else return fn(null, str)
  }
}

Redis.parse = function commandParser (elems, fn) {
  var retval = util.format(formatString1, elems.length)
  while (elems.length) retval += util.format(formatString2, Buffer.byteLength(elems[0]+''), elems.shift()+'')
  if (Redis.debug_mode) console.log('commandParser', retval)
  fn && fn(null, retval)
  return retval
}

Redis.parse.hgetall =
Redis.parse.hmget = function () {
  var hash = {}
    , fields = []
    , vals = []
    , len = 0

  return es.map(function (data, fn) {
    var retval = ''
    if (!(len++ % 2)) fields.push(data)
    else vals.push(String(data))
    if (vals.length === fields.length) {
      return fn(null, [ fields.pop(), vals.pop() ])
    }
    else {
      return fn()
    }
  })
}

module.exports = Redis
