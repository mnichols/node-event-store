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
                cb err
        destroy: (conn) ->
            conn.end()
        max: cfg.maxConnections
        idleTimeoutMillis: 10000
        log: false

    pool.createConnection = ->
        conn = net.createConnection cfg.port, cfg.host

    pool.createProxy = ->
        ending = false
        drained = false
        _write = (cmd) -> 
            return proxy.connection.write(cmd) if proxy.connection
            @pause()
            proxy.connect (err) =>
                proxy.connection.write cmd
                return @resume()
        _end = ->
            console.log 'ended'
            ending = true
            return unless proxy.connection
            pool.release proxy.connection
            proxy.connection.removeAllListeners('data')
            #proxy.connection.removeAllListeners('end')
            #proxy.connection.removeAllListeners('close')
            proxy.connection = null


        _tryEnd = ->
            console.log 'draining', ending
            proxy.connection.end() if ending and proxy.connection
            ending = false
            drained = true
            proxy.connection.removeListener 'drain', _tryEnd

        proxy = es.through _write, _end

        emit = Stream::emit
        proxyEvent = (event) ->
            -> 
                args = Array::slice.call arguments
                args.unshift event
                emit.apply proxy, args
        proxy.release = ->
            return unless proxy.connection
            pool.release proxy.connection
            #proxy.connection.removeAllListeners('data')
            #proxy.connection.removeAllListeners('end')
            #proxy.connection.removeAllListeners('close')
            proxy.connection = null
            
        proxy.connect = (cb = ->) ->
            proxy.pause()
            ending = false
            drained = false
            pool.acquire (err, conn) ->
                return cb err if err?
                proxy.connection = conn
                unless conn.stamped
                    conn.on 'data', proxyEvent 'data'
                    conn.on 'drain', _tryEnd
            #        conn.on 'end', proxyEvent 'end'
            #        conn.on 'close', proxyEvent 'close'
                conn.stamped = true
                cb()
                proxy.resume()
        proxy

    pool
