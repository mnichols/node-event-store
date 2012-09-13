node-event-store
================

This package provides support for an event-sourced persistence strategy leveraging node.js's `Stream` interface.

This exposes the wonderful `pipe` for reading events into and out of Aggregate roots while making it easy to pipe into your publisher(s).

## Supported DBs

Currently, only `redis`.

## Examples

Please see the `test` folder for examples. Especially `event-store.spec.coffee`.


