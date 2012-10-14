describe 'connection-proxy', ->
    sut = require '../connection-pool'
    es = require 'event-stream'
    net = require 'net'

    pool = null
    beforeEach ->
        cfg = 
            port: 6379
            host: 'localhost'
            maxConnections: 10
        pool = sut cfg

    afterEach (done) ->
        pool.drain ->
            pool.destroyAllNow()
            done()
    describe 'when connecting', ->
        connection = null
        afterEach ->
            pool.destroy connection
        it 'should pause stream', (done) ->
            proxy = pool.createProxy()
            proxy.connect (err, conn) ->
                connection= conn
                proxy.paused.should.be.true
                done()

    describe 'when releasing busy connection', ->

        it 'should wait until proxy isnt busy to release', (done) ->
            @timeout 40000
            cmd = '*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n'
            reader = es.readArray [0..1000000].map -> cmd

            proxy = pool.createProxy()
            proxy.connect (err, conn) ->
                reader.pipe proxy
                reader.on 'end', ->
                    proxy.release ->
                        console.log 'size', pool.getPoolSize()
                        proxy.busy.should.be.false
                        done()

    describe 'when writing to proxy', ->

        it 'should connect'
#            pool = sut {db:11}
#            proxy = pool.createProxy()
#            proxy.write '*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue'


        it 'should delegate to underlying connection'

    describe 'when ending proxy', ->
        it 'should keep connections open'

        it 'should defer `end` event until connection is drained'









