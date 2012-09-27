net = require 'net'
poolMod = require 'generic-pool'

module.exports = (cfg) ->
    poolMod.Pool
        name: 'redis'
        create: (cb) ->
            try
                conn = net.createConnection cfg.port, cfg.host
                return cb null, conn
            catch err
                cb err
        destroy: (conn) ->
            conn.end()
        max: 15
        min: 1
        idleTimeoutMillis: 1000
        log: false



