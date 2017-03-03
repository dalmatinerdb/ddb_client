## DalmatinerDB Client

An Erlang TCP client for the dalmatiner time series database.

Firstly, in order to read or write data it is necessary to open a connection:

```erlang
{ok, Con} = ddb_tcp:connect("127.0.0.1", 5555).
```

For efficiency reasons, the Dalmatiner TCP endpoint can only accept incoming data when switched to stream mode.
Therefore, in order to write data points, a connection needs to be swtiched to
stream mode as follows:

```erlang
Bucket = <<"my_bucket">>.
%% The delay is the time in seconds before connection is automatically flushed.
Delay = 1.
{ok, SCon} = ddb_tcp:stream_mode(Bucket, Delay, Con).
```
Values should first be encoded in binary format using the [mmath](https://github.com/dalmatinerdb/mmath) module,
before attempting to write:

```erlang
Timestamp = 1466072419.
Bucket = <<"my_bucket">>.
Metric = dproto:metric_from_list([<<"base">>, <<"cpu">>]).
Value  = mmath_bin:from_list([63.9, 64.0, 64.1]).
{ok, SCon1} = ddb_tcp:send(Metric, Timestamp, Value, SCon).
```

A stream connection is not required in order to perform read operations.  In
addition to listing metrics and buckets, data for a given point in time can
be read as follows:

```erlang
Timestamp = 1466072419.
Bucket = <<"my_bucket">>.
Metric = dproto:metric_from_list([<<"base">>, <<"cpu">>]).
Count = 3.
{ok, Data, Con1} = ddb_tcp:get(Bucket, Metric, Timestamp, Count, Con).
```

Finally, a connection should be closed when done:
```erlang
Con1 = ddb_tcp:close(Con).
```

For more information, please consult the [network protocol
documentation](http://dalmatinerdb.readthedocs.io/en/latest/proto.html).

## Read options
It is possible to specify consistency parameters for an individual read operation if so
required.  The following options are available:

Read repair: `{rr, off}` - disables read repair for this get request
             `{rr, on}` - ensures read repairs for this read request, even on recently written data
             `{rr, default}` - will use the system defaults

Read quorum: `{r, n}` - will use the same value as N for this get request
             `{r, R :: pos_integer()}` - will use the value 0 < R <= N for this get request
             `{r, default}` - will use the system default for R

To use these options in a read request:
```erlang
Timestamp = 1466072419.
Bucket = <<"my_bucket">>.
Metric = dproto:metric_from_list([<<"base">>, <<"cpu">>]).
Count = 3.
Opts = [{rr, off}, {r, 1}].
{ok, Data, Con1} = ddb_tcp:get(Bucket, Metric, Timestamp, Count, Opts, Con).
```

## Connection pools

A DalmatinerDB connection is dedicated to send data to a single bucket.  In cases
where data is written to many buckets at once, it is more efficient to
re-use an existing connection for a bucket instead of opening a new one.
Applications such as [Pooler](https://github.com/seth/pooler) may be used to
pool and share connections to DDB.

## Building the client

The client can be compiled using `rebar3`.

Linting rules are specified using the Elvis plugin, and lint rules may be
checked by running:
```
$ rebar3 as lint lint
```
