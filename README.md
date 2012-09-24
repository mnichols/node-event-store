node-event-store
================

This package provides support for an event-sourced persistence strategy leveraging node.js's `Stream` interface.

This exposes the wonderful `pipe` for reading events into and out of Aggregate roots while making it easy to pipe into your publisher(s).

## Install

`npm install node-event-store`

## Supported DBs

Currently, only `redis`.

`npm install node-event-store-redis`

## Examples

### A typical command handler

``` js

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

```

Please see the `test` folder for examples. Especially `event-store.spec.coffee`.


## Testing

    make test

Or for integration testing

    make TEST_TYPE=integration test

## TODO

See issues



## License

Copyright (c) 2012 Mike Nichols

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
