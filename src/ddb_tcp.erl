%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc This module provides a wrapper around DalmatinerDB's TCP
%%% protocol. It can be used for both sending and requesting data and
%%% does support the streaming send mode for TCP.
%%%
%%% The {@link connect/2} call will return a new connection with the
%%% required information stored. It also contains a gen_tcp connection
%%% so the same limits of ownership apply here.
%%%
%%% The connection is not guaranteed to be alive all the time.
%%% In the case of a failure an attempt will be made to re-establish the
%%% connection before forwarding the error to the caller.
%%%
%%% Once entering the stream mode by calling {@link stream/2} only
%%% the send command is supported, other commands will cause an error,
%%% however not disconnect the system.
%%%
%%% @end
%%% Created : 15 Dec 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(ddb_tcp).

-include_lib("dproto/include/dproto.hrl").
-define(TIMEOUT, 30000).

-export([
         connect/2,
         connect/1,
         mode/1,
         connected/1,
         close/1,
         stream_mode/3,
         stream_mode/4,
         bucket_info/2,
         list/1,
         list/2,
         list/3,
         get/5,
         set_ttl/3,
         send/4,
         batch_start/2,
         batch/2,
         batch/3,
         batch_end/1
        ]).

-ignore_xref([batch/2, batch/3, batch_start/2, batch_end/1,
              batch_start/2, bucket_info/2, close/1,
              connect/1, connect/2, connected/1, get/5,
              list/1, list/2, list/3, mode/1, send/4,
              set_ttl/3, stream_mode/3, stream_mode/4]).

-export_type([connection/0]).

-type socket() :: port().

-type ttl() :: pos_integer() | infinity.

-record(ddb_connection,
        {socket :: socket() | undefined,
         host,
         port,
         mode = normal,
         bucket,
         resolution :: pos_integer(),
         error = none,
         delay = 1,
         batch = false}).

%%--------------------------------------------------------------------
%% @type connection().
%%
%% A connection to the DalmatinerDB backend server.
%% @end
%%--------------------------------------------------------------------
-opaque connection() :: #ddb_connection{}.

-define(OPTS, [binary, {packet, 4}, {active, false}]).

%%--------------------------------------------------------------------
%% @doc Connects to a DalmatinerDB instance. It will try to create a
%% gen_tcp connection however it will return successfully even if the
%% gen_tcp connection could not be established!
%%
%% To test for connection use {@link connected/1}.
%%
%% @end
%%--------------------------------------------------------------------
-spec connect(Host :: inet:ip_address() | inet:hostname(),
              Port :: inet:port_number()) ->
                     {ok, connection()}.

connect(Host, Port) ->
    case gen_tcp:connect(Host, Port, ?OPTS, 500) of
        {ok, Socket} ->
            {ok, #ddb_connection{
                    socket = Socket,
                    host = Host,
                    port = Port
                   }};
        {error, E} ->
            {ok, #ddb_connection{
                    socket = undefined,
                    host = Host,
                    port = Port,
                    error = E
                   }}
    end.

%%--------------------------------------------------------------------
%% @doc Forces a reconnect on a disconnected connection, has no effect
%% on connections that are still connected.
%%
%% @end
%%--------------------------------------------------------------------
-spec connect(Connection :: connection()) ->
                     {ok, connection()}.

connect(Con) ->
    {ok, reconnect(Con)}.

%%--------------------------------------------------------------------
%% @doc Shows what mode the connection is in and if in stream mode
%% which bucket the data is streamed to.
%%
%% @end
%%--------------------------------------------------------------------
-spec mode(Connection :: connection()) ->
                  {ok, normal} |
                  {ok, {stream, Bucket :: binary()}} |
                  {ok, {batch, Bucket :: binary()}}.

mode(#ddb_connection{mode = stream, bucket=Bucket, batch = true}) ->
    {ok, {batch, Bucket}};
mode(#ddb_connection{mode = stream, bucket=Bucket}) ->
    {ok, {stream, Bucket}};
mode(#ddb_connection{mode = normal}) ->
    {ok, normal}.

%%--------------------------------------------------------------------
%% @doc Shows whether a connection is currently connected to the backend or
%% awaiting reconnect.
%%
%% @end
%%--------------------------------------------------------------------
-spec connected(Connection :: connection()) ->
                       boolean().

connected(#ddb_connection{socket = undefined}) ->
    false;
connected(_) ->
    true.

%%--------------------------------------------------------------------
%% @doc Puts a connection into stream mode.  If the connection is already in
%% stream mode, an error is returned unless the requested stream parameters
%% are equal to the current ones.
%%
%% @end
%%--------------------------------------------------------------------
-spec stream_mode(Bucket :: binary(),
                  Delay :: pos_integer(),
                  Connection :: connection()) ->
                         {ok, Connection :: connection()} |
                         {error, Error :: inet:posix(),
                          Connection :: connection()} |
                         {error, {stream, OldBucket :: binary(),
                                  OldDelay :: pos_integer()},
                          Connection :: connection()}.

stream_mode(Bucket, Delay, Con = #ddb_connection{mode = stream,
                                                 bucket = Bucket,
                                                 delay = Delay}) ->
    {ok, Con};

stream_mode(_Bucket, _Delay, Con = #ddb_connection{mode = stream,
                                                   bucket = OldBucket,
                                                   delay = OldDelay}) ->
    {error, {stream, OldBucket, OldDelay}, Con};

stream_mode(Bucket, Delay, Con) ->
    Bin = dproto_tcp:encode({stream, Bucket, Delay}),
    Con1 = Con#ddb_connection{mode = stream,
                              bucket = Bucket,
                              delay = Delay},
    case send_bin(Bin, Con1) of
        {ok, Con2} ->
            {ok, reset_state(Con2)};
        E ->
            E
    end.

-spec stream_mode(Bucket :: binary(),
                  Delay :: pos_integer(),
                  Resolution :: pos_integer(),
                  Connection :: connection()) ->
                         {ok, Connection :: connection()} |
                         {error, Error :: inet:posix(),
                          Connection :: connection()} |
                         {error, {stream, OldBucket :: binary(),
                                  OldDelay :: pos_integer(),
                                  OldRes :: pos_integer()},
                          Connection :: connection()}.

stream_mode(Bucket, Delay, Res, Con = #ddb_connection{mode = stream,
                                                      bucket = Bucket,
                                                      delay = Delay,
                                                      resolution = Res}) ->
    {ok, Con};

stream_mode(_Bucket, _Delay, _Res, Con = #ddb_connection{mode = stream,
                                                         resolution = OldRes,
                                                         bucket = OldBucket,
                                                         delay = OldDelay}) ->
    {error, {stream, OldBucket, OldDelay, OldRes}, Con};


stream_mode(Bucket, Delay, Res, Con) ->
    Bin = dproto_tcp:encode({stream, Bucket, Delay, Res}),
    Con1 = Con#ddb_connection{mode = stream,
                              bucket = Bucket,
                              resolution = Res,
                              delay = Delay},
    case send_bin(Bin, Con1) of
        {ok, Con2} ->
            {ok, reset_state(Con2)};
        E ->
            E
    end.

%%--------------------------------------------------------------------
%% @doc Starts a batch transfer for a given timeslot.
%% Once started, additional metrics with the same time can be sent via the
%% {@link batch/2} and {@link batch/3} functions.
%%
%% @end
%%--------------------------------------------------------------------
-spec batch_start(Time :: non_neg_integer(), Connection :: connection()) ->
                         {ok, Connection :: connection()} |
                         {error, {batch, Time :: non_neg_integer()},
                          Connection :: connection()} |
                         {error, Error :: inet:posix(),
                          Connection :: connection()} |
                         {error, {bad_mode, normal},
                          Connection :: connection()}.
batch_start(_Time, Con = #ddb_connection{batch = Time}) when is_integer(Time) ->
    {error, {batch, Time}, Con};
batch_start(_Time, Con = #ddb_connection{mode = normal}) ->
    {error, {bad_mode, normal}, Con};
batch_start(Time, Con) when
      is_integer(Time),
      Time >= 0 ->
    Con1 = Con#ddb_connection{batch = Time},
    Bin = dproto_tcp:encode({batch, Time}),
    case send_bin(Bin, Con1) of
        {ok, Con2} ->
            {ok, Con2};
        E ->
            E
    end.

%%--------------------------------------------------------------------
%% @doc Sends a batch of multiple values with a single tcp call.
%% @end
%%--------------------------------------------------------------------
-spec batch([{Metric :: binary() | [binary()], Point :: integer() | binary()}],
             Connection :: connection()) ->
                   {ok, Connection :: connection()} |
                   {error, Error :: inet:posix(), Connection :: connection()} |
                   {error, no_batch, Connection :: connection()}.
batch(MPs, Con = #ddb_connection{batch = _Time})
  when is_integer(_Time),
       is_list(MPs) ->
    Bin = << <<(to_batch(Metric, Point))/binary>> || {Metric, Point} <- MPs >>,
    case send_bin(Bin, Con) of
        {ok, Con1} ->
            {ok, Con1};
        E ->
            E
    end;

batch(_MPs, Con) ->
    {error, no_batch, Con}.

%%--------------------------------------------------------------------
%% @doc Sends a single metric value pair for a batch
%% @end
%%--------------------------------------------------------------------
-spec batch(Metric :: binary() | [binary()],
            Point :: integer() | binary(),
            Connection :: connection()) ->
                  {ok, Connection :: connection()} |
                  {error, Error :: inet:posix(), Connection :: connection()} |
                  {error, no_batch, Connection :: connection()}.



batch(Metric, Point, Con) when is_integer(Point) ->
    batch(Metric, mmath_bin:from_list([Point]), Con);

batch([_M | _] = Metric, Point, Con) when is_binary(_M) ->
    batch(dproto:metric_from_list(Metric), Point, Con);

batch(Metric, Point, Con = #ddb_connection{batch = _Time})
  when is_binary(Metric),
       is_binary(Point),
       is_integer(_Time) ->
    Bin = dproto_tcp:encode({batch, Metric, Point}),
    case send_bin(Bin, Con) of
        {ok, Con1} ->
            {ok, Con1};
        E ->
            E
    end;
batch(_Metric, _Point, Con) ->
    {error, no_batch, Con}.


%%--------------------------------------------------------------------
%% @doc Finalizes the batch transfer.
%% @end
%%--------------------------------------------------------------------
-spec batch_end(Connection :: connection()) ->
                       {error, Error :: inet:posix(),
                        Connection :: connection()} |
                       {ok, Connection :: connection()}.

batch_end(Con = #ddb_connection{batch = _Time}) when is_integer(_Time) ->
    Con1 = Con#ddb_connection{batch = false},
    Bin = dproto_tcp:encode(batch_end),
    case send_bin(Bin, Con1) of
        {ok, Con2} ->
            {ok, Con2};
        E ->
            E
    end;
batch_end(Con) ->
    Con1 = Con#ddb_connection{batch = false},
    {ok, Con1}.

%%--------------------------------------------------------------------
%% @doc Reads the metadata properties of a bucket.
%%
%% @end
%%--------------------------------------------------------------------
-spec bucket_info(Bucket :: binary(), Connection :: connection()) ->
                  {ok, {Res :: pos_integer(), PPF :: pos_integer(),
                        TTL :: ttl()}, Connection :: connection()} |
                  {error, stream, Connection :: connection()}.

bucket_info(Bucket, Con =  #ddb_connection{mode = normal}) ->
    case send_bin(dproto_tcp:encode({info, Bucket}), Con) of
        {ok, Con1 = #ddb_connection{socket = Socket}} ->
            case gen_tcp:recv(Socket, 0, ?TIMEOUT) of
                {ok, InfoBin} ->
                    {ok, dproto_tcp:decode_bucket_info(InfoBin), Con1};
                {error, E} ->
                    {error, E, close(Con1)}
            end;
        E ->
            E
    end;

bucket_info(_Bucket, Con) ->
    {error, stream, Con}.

%%--------------------------------------------------------------------
%% @doc Retrieves a list of all buckets on the server. Returns an error
%% when in stream mode.
%%
%% @end
%%--------------------------------------------------------------------
-spec list(Connection :: connection()) ->
                  {ok, [Bucket :: binary()], Connection :: connection()} |
                  {error, stream, Connection :: connection()}.

list(Con =  #ddb_connection{mode = normal}) ->
    do_list(send_bin(dproto_tcp:encode(buckets), Con));

list(Con) ->
    {error, stream, Con}.

%%--------------------------------------------------------------------
%% @doc Retrieves a list of all metrics in a bucket. Returns an error
%% when in stream mode.
%%
%% @end
%%--------------------------------------------------------------------
-spec list(Bucket :: binary(), Connection :: connection()) ->
                  {ok, [Metric :: binary()], Connection :: connection()} |
                  {error, stream, Connection :: connection()}.

list(Bucket, Con =  #ddb_connection{mode = normal}) ->
    do_list(send_bin(dproto_tcp:encode({list, Bucket}), Con));

list(_Bucket, Con) ->
    {error, stream, Con}.

%%--------------------------------------------------------------------
%% @doc Retrieves a list of all metrics with a given prefix. Returns an
%% error when in stream mode.
%%
%% @end
%%--------------------------------------------------------------------
-spec list(Bucket :: binary(), Prefix :: binary(),
           Connection :: connection()) ->
                  {ok, [Metric :: binary()], Connection :: connection()} |
                  {error, stream, Connection :: connection()}.

list(Bucket, Prefix, Con =  #ddb_connection{mode = normal}) ->
    do_list(send_bin(dproto_tcp:encode({list, Bucket, Prefix}), Con));

list(_Bucket, _Prefix, Con) ->
    {error, stream, Con}.

%%--------------------------------------------------------------------
%% @doc Retrieves a range of data from a metric. Returns an error when
%% in stream mode.
%%
%% @end
%%--------------------------------------------------------------------
-spec get(Bucket :: binary(),
          Metric :: binary(),
          Time :: pos_integer(),
          Count :: pos_integer(),
          Connection :: connection()) ->
                 {ok, {Resolution :: pos_integer(),
                       Data :: binary()},
                  Connection :: connection()} |
                 {error, Error :: inet:posix(), Connection :: connection()} |
                 {error, stream, Connection :: connection()}.

get(Bucket, Metric, Time, Count, Con =  #ddb_connection{mode = normal}) ->
    case send_bin(dproto_tcp:encode({get, Bucket, Metric, Time, Count}), Con) of
        {ok, Con1 = #ddb_connection{socket = Socket}} ->
            case gen_tcp:recv(Socket, 0, ?TIMEOUT) of
                {ok, <<Resolution:64/integer, D/binary>>} ->
                    {ok, {Resolution, D}, Con1};
                {error, E} ->
                    {error, E, close(Con1)}
            end;
        E ->
            E
    end;

get(_, _, _, _, Con) ->
    {error, stream, Con}.

%%--------------------------------------------------------------------
%% @doc Sets the TTL (expiry) for a given bucket.  This defines the length
%% of time for which data points are stored before remove by the vacuum.  A
%% value of `infinity' means data is retained indefinitely.
%% The TTL for a bucket may also be set via the ddb admin console.
%%
%% @end
%%--------------------------------------------------------------------
-spec set_ttl(Bucket :: binary(), TTL :: ttl(), Connection :: connection()) ->
                {error, Error :: inet:posix(), Connection :: connection()} |
                {error, {bad_ttl, TTL :: ttl()}, Connection :: connection()} |
                {error, {bad_mode, stream}, Connection :: connection()} |
                {ok, Connection :: connection()}.

set_ttl(_Bucket, _TTL, Con = #ddb_connection{mode = stream}) ->
    {error, {bad_mode, stream}, Con};
set_ttl(_Bucket, TTL, Con) when
      is_integer(TTL), TTL =< 0 ->
    {error, {bad_ttl, TTL}, Con};
set_ttl(Bucket, TTL, Con) when
      is_integer(TTL);TTL =:= infinity ->
    Bin = dproto_tcp:encode({ttl, Bucket, TTL}),
    case send_bin(Bin, Con) of
        {ok, Con1} ->
            {ok, Con1};
        E ->
            E
    end.

%%--------------------------------------------------------------------
%% @doc Sends data to the server on streaming mode. Returns an error
%% when in batch mode.
%%
%% @end
%%--------------------------------------------------------------------
-spec send(Metric :: binary() | [binary()],
           Time :: pos_integer(),
           Points :: [integer()] | binary(),
           Connection :: connection()) ->
                  {ok, Connection :: connection()} |
                  {error, Error :: inet:posix(), Connection :: connection()} |
                  {error, no_stream, Connection :: connection()}.


send([_M | _] = Metric, Time, Points, Con =  #ddb_connection{mode = stream})
  when is_binary(_M) ->
    send(dproto:metric_from_list(Metric), Time, Points, Con);

send(_, _, _, Con = #ddb_connection{batch = Time}) when is_integer(Time) ->
    {error, {batch, Time}, Con};

send(Metric, Time, Points, Con = #ddb_connection{mode = stream}) ->
    send_bin(dproto_tcp:encode({stream, Metric, Time, Points}), Con);

send(_, _, _, Con) ->
    {error, no_stream, Con}.

%%--------------------------------------------------------------------
%% @doc Forces a connection to close.
%%
%% @end
%%--------------------------------------------------------------------
-spec close(Connection :: connection()) ->
                   Connection :: connection().

close(Con = #ddb_connection{socket = undefined}) ->
    Con;

close(Con = #ddb_connection{socket = Sock}) ->
    gen_tcp:close(Sock),
    Con#ddb_connection{socket = undefined}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

send_bin(Bin, Con = #ddb_connection{socket = undefined}) ->
    send1(Bin, reconnect(Con));

send_bin(Bin, Con = #ddb_connection{socket = Sock}) ->
    case gen_tcp:send(Sock, Bin) of
        {error, _E} ->
            send1(Bin, reconnect(close(Con)));
        _ ->
            {ok, Con}
    end.

send1(_Bin, Con = #ddb_connection{socket = undefined, error = E}) ->
    {error, E, Con};

send1(Bin, Con = #ddb_connection{socket = Sock}) ->
    case gen_tcp:send(Sock, Bin) of
        {error, E} ->
            {error, E, close(Con)};
        _ ->
            {ok, Con}
    end.

-spec reconnect(connection()) ->
                       connection().
reconnect(Con = #ddb_connection{socket = undefined,
                                host = Host,
                                port = Port}) ->
    case gen_tcp:connect(Host, Port, ?OPTS, 500) of
        {ok, Socket} ->
            reset_stream(Con#ddb_connection{socket = Socket, error = none});
        {error, E} ->
            Con#ddb_connection{error = E}
    end;

reconnect(Con) ->
    Con.

reset_stream(Con = #ddb_connection{socket = _S,
                                   mode = stream,
                                   bucket = Bucket,
                                   delay = Delay}) when _S /= undefined ->
    Bin = dproto_tcp:encode({stream, Bucket, Delay}),
    case send_bin(Bin, Con) of
        {ok, Con1} ->
            reset_batch(reset_state(Con1));
        E ->
            E
    end;

reset_stream(Con) ->
    reset_state(Con).

reset_batch(Con = #ddb_connection{batch = Time}) when is_integer(Time) ->
    Bin = dproto_tcp:encode({batch, Time}),
    case send_bin(Bin, Con) of
        {ok, Con1} ->
            Con1;
        E ->
            E
    end;

reset_batch(Con) ->
    Con.

reset_state(Con = #ddb_connection{socket = Socket, mode = stream})
  when Socket /= undefined ->
    inet:setopts(Socket, [{packet, 0}]),
    Con;

reset_state(Con) ->
    Con.

decode_metrics(<<>>, Acc) ->
    Acc;

decode_metrics(<<S:16/integer, M:S/binary, R/binary>>, Acc) ->
    decode_metrics(R, [M | Acc]).

do_list({ok, Con1 = #ddb_connection{socket = S}}) ->
    case gen_tcp:recv(S, 0, ?TIMEOUT) of
        {ok, <<Size:?METRICS_SS/?SIZE_TYPE, Reply:Size/binary>>} ->
            {ok, decode_metrics(Reply, []), Con1};
        {error, E} ->
            {error, E, Con1}
    end;

do_list(Error) ->
    Error.

to_batch(Metric, Point) when is_integer(Point) ->
    to_batch(Metric, mmath_bin:from_list([Point]));

to_batch([_M | _] = Metric, Point) when is_binary(_M) ->
    to_batch(dproto:metric_from_list(Metric), Point);

to_batch(Metric, Point)
  when is_binary(Metric),
       is_binary(Point) ->
    dproto_tcp:encode({batch, Metric, Point}).
