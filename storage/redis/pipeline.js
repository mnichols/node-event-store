var es = require('event-stream')
//
// combine multiple streams together so that they act as a single stream
// accepts opts as first argument, with default of {rethrow: true}
// which rethrows the underlying error emits on the main pipe
//
module.exports = function () {

  var streams = [].slice.call(arguments)
  if(!streams[0].pipe) {
      opts = streams.shift()
  }

  var first = streams[0]
    , last = streams[streams.length - 1]
    , thepipe = es.duplex(first, last)
    , opts = opts || { rethrow: true }


  if(streams.length == 1)
    return streams[0]
  else if (!streams.length)
    throw new Error('connect called with empty args')

  //pipe all the streams together

  function recurse (streams) {
    if(streams.length < 2)
      return
    streams[0].pipe(streams[1])
    recurse(streams.slice(1))  
  }
  
  recurse(streams)
 
  function onerror () {
    var args = [].slice.call(arguments)
    args.unshift('error')
    thepipe.emit.apply(thepipe, args)
  }
  
  if(opts.rethrow) {
    streams.forEach(function (stream) {
      stream.on('error', onerror)
    })
  }

  return thepipe
}

