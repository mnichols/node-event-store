{Stream} = require 'stream'
es = require 'event-stream'
module.exports = (target) ->
    enabled = false
    ops = ['write','end','pause','resume']
    events = ['end', 'data', 'commit', 'drain', 'readable']
    proxy = new Stream()
    proxy.writable = target.writable
    proxy.readable = target.readable
    reemit = (e) ->
        target.on e, ->
            args = Array::slice.call arguments
            args.unshift e
            proxy.emit.apply proxy, args
    for op in ops
        proxy[op] = target[op] if target[op]
    (reemit e) for e in events
    check = es.map (data, next) ->
        return next null, data if enabled
        disabled = new Error 'proxy has not been enabled'
        return next disabled
    proxy.pipe = (dest) ->
        target.pipe(check).pipe(dest)
    proxy.write = (data) ->
        args = Array::slice.call arguments
        target.write.apply target, args
    proxy.enable = ->
        enabled = true
    proxy





