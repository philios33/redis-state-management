# Redis State Management

This is a collection of utility classes to help speak to a Redis backed storage engine.

## Writing

All write actions arrive via a single Redis queue so that diffs can be calculated for listening subscribers that are only interested in deltas/changes.  Rather than hosting a HTTP service, producers append writes directly to the incoming queue themselves, cutting out the unnecessary service.  A processor processes every write command in near real time.  This allows synchronised writes to versioned state variables (variables that track changes).

## Reading

Apart from unserializing objects, there is nothing special about reading data from storage variables.  The exported lib classes help with this communication and wrap the redis calls.  Subscribing to listen for state changes is also possible.

