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

Please see the `test` folder for examples. Especially `event-store.spec.coffee`.


## Testing

    make test

Or for integration testing

    make TEST_TYPE=integration test
