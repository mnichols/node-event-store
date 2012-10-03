net = require 'net'
poolMod = require 'generic-pool'
es = require 'event-stream'
{Stream} = require 'stream'

module.exports = (cfg) ->
    id = 0
    pool = poolMod.Pool
        name: 'redis'
        create: (cb) ->
            try
                conn = pool.createConnection()
                conn.id = id++
                return cb null, conn
            catch err
                console.error err
                cb err
        destroy: (conn) ->
            conn.end()
        max: cfg.maxConnections
        idleTimeoutMillis: 10000
        log: false

    pool.createConnection = ->
        conn = net.createConnection cfg.port, cfg.host

    pool.createProxy = ->

        buffer = []
        ended = false
        destroyed = false
        proxy = new Stream()
        proxy.readable = true
        proxy.writable = true
        proxy.paused = false
        proxy.buffer = buffer



        proxy.on 'end', ->
            proxy.readable = false
            return if proxy.writable
            process.nextTick ->
                proxy.destroy()

        _end = ->
            proxy.writable = false
            if proxy.connection
                proxy.connection.end.call proxy.connection

        proxy.end = ->
            return if ended
            ended = true
            proxy.writable = false
            proxy.write.call proxy if arguments.length
            proxy.release ->
                process.nextTick ->
                    proxy.emit 'end'
            return if proxy.readable
            proxy.destroy()
            

        proxy.destroy = ->
            return if destroyed
            destroyed = true
            ended = true
            buffer.length = 0
            stream.writable = stream.readable = false
            stream.emit 'close'
        proxy.pause = ->
            proxy.paused = true
        proxy.resume = ->
            proxy.paused = false

        proxy.write = (data) ->
            proxy.pause()
            unless proxy.writer
                throw new Error 'proxy `connect` has not been called' 
            proxy.writer.write data
            proxy.resume()

        proxyEvent = (event) ->
            -> 
                args = Array::slice.call arguments
                args.unshift event
                emit.apply proxy, args
        proxy.release = (cb = ->) ->
            return cb() unless proxy.writer
            _release = ->
                if proxy.connection
                    pool.release proxy.connection
                proxy.busy = false
                proxy.connection = null
                proxy.writer = null
                cb()
            
            if proxy.busy
                return proxy.once 'releasable', _release
            _release()

            

        proxy.connect = (cb = ->) ->
            proxy.pause()
            pool.acquire (err, conn) ->
                return cb err if err?
                conn.removeAllListeners()
                before = es.map (cmd, next) ->
                    proxy.busy = true
                    next null, cmd
                    
                after = es.through (reply) ->
                    proxy.busy = false
                    proxy.emit 'data', reply
                    proxy.emit 'releasable'

                proxy.connection = conn
                proxy.busy = false
                proxy.writer = es.pipeline before,
                    proxy.connection,
                    after

                cb null, conn
                proxy.resume()
        proxy



#    pool.createProxy = ->
#        ending = false
#        drained = false
#        _write = (cmd) -> 
#            return proxy.connection.write(cmd) if proxy.connection
#            @pause()
#            proxy.connect (err) =>
#                proxy.connection.write cmd
#                return @resume()
#        _end = ->
#            console.log 'ended'
#            ending = true
#            return unless proxy.connection
#            pool.release proxy.connection
#            proxy.connection.removeAllListeners('data')
#            #proxy.connection.removeAllListeners('end')
#            #proxy.connection.removeAllListeners('close')
#            proxy.connection = null
#
#
#        _tryEnd = ->
#            console.log 'draining', ending
#            proxy.connection.end() if ending and proxy.connection
#            ending = false
#            drained = true
#            proxy.connection.removeListener 'drain', _tryEnd
#
#        proxy = es.through _write, _end
#
#        emit = Stream::emit
#        proxyEvent = (event) ->
#            -> 
#                args = Array::slice.call arguments
#                args.unshift event
#                emit.apply proxy, args
#        proxy.release = ->
#            return unless proxy.connection
#            pool.release proxy.connection
#            #proxy.connection.removeAllListeners('data')
#            #proxy.connection.removeAllListeners('end')
#            #proxy.connection.removeAllListeners('close')
#            proxy.connection = null
#            
#        proxy.connect = (cb = ->) ->
#            proxy.pause()
#            ending = false
#            drained = false
#            pool.acquire (err, conn) ->
#                return cb err if err?
#                proxy.connection = conn
#                unless conn.stamped
#                    conn.on 'data', proxyEvent 'data'
#                    conn.on 'drain', _tryEnd
#            #        conn.on 'end', proxyEvent 'end'
#            #        conn.on 'close', proxyEvent 'close'
#                conn.stamped = true
#                cb()
#                proxy.resume()
#        proxy

    pool
