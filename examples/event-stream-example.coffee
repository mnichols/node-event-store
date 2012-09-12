es = require '../event-stream'
redis-storage = require '../storage/redis'
readSomeEvents = ->
    store = es redis-storage
    filter = 
        streamId: '123'
        minRevision: 0
        maxRevision: Number.MAX_VALUE
    stream = store.open filter
    stream.on 'data', (events) ->

    stream.read()
