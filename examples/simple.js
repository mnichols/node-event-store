var eventStore = require('node-event-store')
var redisStrategy = require('node-event-store-redis')
var RedisClient = require('redis-stream')
var eventStream = require('event-stream')


function aTypicalCommandHandler () {
    var store = initializeEventStore()

    //by default this will pull
    //all events using
    //minRevision = 0 && maxRevision=Number.MAX_VALUE
    //streamId is typically your Aggregate Root id
    var filter = {
        streamId: '123'
    }

    var stream = store.open(filter);
    //this bit is handled by _you_ according
    //to your domain reqs
    var aggregate = createAggregate()
    var commandHandler = eventStream.map(phonyAggregateOperation)
    var publisher = eventStream.through(publishTheCommit)
    stream.pipe(aggregate).pipe(commandHandler).pipe(stream.commit()).pipe(publisher)

}

function initializeEventStore () {
    //using database 11 of local redis
    //for now, the branch of redis-stream
    //at github.com/mnichols/redis-stream/tarball/reply
    //but will fold into event-store
    //see https://github.com/mnichols/node-event-store/issues/8
    var redisCli = new RedisClient(6379, 'localhost', 11)
    var redisStorageCfg = {
        client: redisCli
    }
    var redis = redisStrategy.createStorage(redisStorageCfg)
    var auditor = redisStrategy.createAuditor(redisStorageCfg)
    //include the auditor to get support for
    //streaming all events in the store.
    //Handy for rebuilding view models or audits
    var store = eventStore(redis, auditor) 

}

function createAggregate() {
    //it's up to you to provide a writable stream
    //that receives the events
    //node-event-store will hand you.
    //For this example, we are just buffering them all
    //and then passing them into a another filter.
    //From there, we are simply passing on the aggregate root
    //but obviously you need to apply the events to your aggregate first :)
    var AggregateRoot = function () { }
    var ar = new AggregateRoot()
    var poolEvents = require('./bucket-stream')
    var mapEventsToAgg = eventStream.map(function (events, next) {
        ar.events = events
        next(null, ar);
    })
    eventStream.pipeline(poolEvents, mapEventsToAgg)
}

function phonyAggregateOperation (agg, next) {
    //do some stuff with aggregate
    //and expose events 
    next(null, {some:'event'})
}

function publishTheCommit (commit) {
    //commit.payload is the Array of events
    //do stuff here

}

