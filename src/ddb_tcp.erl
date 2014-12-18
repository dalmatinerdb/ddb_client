%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc This module provides a wrapper around DalmatinerDB's TCP
%%% protocol. It can be used for both sending and requesting data and
%%% does support the streaming send mode for TCP.
%%%
%%% The {@link connect/2} call will return a new connection with the
%%% required information stored. It also contens a gen_tco connection
%%% so the same limits of ownership apply here.
%%%
%%% The connection is not guarantted to be alive at all the time and
%%% in the case of a failure will be tried to be reestablished before
%%% forwarding the error to the caller.
%%%
%%% Once entering the stream mode by calling {@link stream/2} only
%%% the send command is supported, other commands will cause an error,
%%% however not disconect the system.
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
         list/1,
         list/2,
         get/5,
         send/4
        ]).

-export_type([connection/0]).

-record(ddb_connection,
        {socket :: gen_tcp:socket() | undefined,
         host,
         port,
         mode = normal,
         bucket,
         error = none,
         delay = 1}).

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
%% gen_tcp connection however it will return successfully even if that
%% could not be established!
%%
%% To test for connection use {@link connected/1}.
%%
%% @spec connect(Host :: inet:ip_address() | inet:hostname(),
%%               Port :: inet:port_number()) ->
%%         connection()
%% @end
%%--------------------------------------------------------------------

connect(Host, Port) ->
    case gen_tcp:connect(Host, Port, ?OPTS) of
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
%% @spec connect(Connection :: connection()) ->
%%         connection()
%% @end
%%--------------------------------------------------------------------

connect(Con) ->
    reconnect(Con).

%%--------------------------------------------------------------------
%% @doc Shows what mode the connection is in and if in stream mode
%% which bucket the data is streamed to.
%%
%% @spec mode(Connection :: connection()) ->
%%         {ok, normal} |
%%         {ok, {stream, Bucket :: binary()}}
%% @end
%%--------------------------------------------------------------------
mode(#ddb_connection{mode = stream, bucket=Bucket}) ->
    {ok, {stream, Bucket}};
mode(#ddb_connection{mode = normal}) ->
    {ok, normal}.

%%--------------------------------------------------------------------
%% @doc Shows weather a connection is currently connected to the kback
%% backend or awaiting reconncet.
%%
%% @spec connected(Connection :: connection()) ->
%%         boolean()
%% @end
%%--------------------------------------------------------------------
connected(#ddb_connection{socket = undefined}) ->
    false;
connected(_) ->
    true.

%%--------------------------------------------------------------------
%% @doc Puts a connection into stream mode, if the connection was in
%% stream mode before an error is returned unless the requested stream
%% parameters are equal to the current ones.
%%
%% @spec stream_mode(Bucket :: binary(),
%%                   Delay :: pos_integer(),
%%                   Connection :: connection()) ->
%%         {ok, Connection :: connection()} |
%%         {error, {stream, OldBucket :: binary(),
%%                          OldDelay :: pos_integer()}}
%% @end
%%--------------------------------------------------------------------
stream_mode(Bucket, Delay, Con = #ddb_connection{mode = stream,
                                                 bucket = Bucket,
                                                 delay = Delay}) ->
    {ok, Con};
stream_mode(_Bucket, _Delay, #ddb_connection{mode = stream,
                                             bucket = OldBucket,
                                             delay = OldDelay}) ->
    {error, {stream, OldBucket, OldDelay}};

stream_mode(Bucket, Delay, Con) ->
    Bin = dproto_tcp:encode_start_stream(Bucket, Delay),
    Con1 = Con#ddb_connection{mode = stream,
                              bucket = Bucket,
                              delay = Delay},
    case send_bin(Bin, Con1) of
        {ok, Con2} ->
            {ok, reset_state(Con2)};
        E ->
            E
    end.

%%--------------------------------------------------------------------
%% @doc Retrives a list fo all buckets on the srever. Returns an error
%% when in stream mode.
%%
%% @spec list(Connection :: connection()) ->
%%         {ok, [Bucket :: binary()], Connection :: connection()} |
%%         {error, stream}
%% @end
%%--------------------------------------------------------------------
list(Con =  #ddb_connection{mode = normal}) ->
    do_list(send_bin(dproto_tcp:encode(buckets), Con));

list(_Con) ->
    {error, stream}.

%%--------------------------------------------------------------------
%% @doc Retrives a list fo all metrics in a bucket. Returns an error
%% when in stream mode.
%%
%% @spec list(Bucket :: binary(), Connection :: connection()) ->
%%         {ok, [Metric :: binary()], Connection :: connection()} |
%%         {error, stream}
%% @end
%%--------------------------------------------------------------------
list(Bucket, Con =  #ddb_connection{mode = normal}) ->
    do_list(send_bin(dproto_tcp:encode({list, Bucket}), Con));

list(_Bucket, _Con) ->
    {error, stream}.

%%--------------------------------------------------------------------
%% @doc Retrives a range of data from a metric or an error when in
%% stream mode.
%%
%% @spec get(Bucket :: binary(),
%%           Metric :: binary(),
%%           Time :: pos_integer(),
%%           Count :: pos_integer(),
%%           Connection :: connection()) ->
%%         {ok, {Resolution :: pos_integer(),
%%               Data :: binary()},
%%              Connection :: connection()} |
%%         {error, Error :: inet:posix(), Connection :: connection()} |
%%         {error, stream}
%% @end
%%--------------------------------------------------------------------

get(Bucket, Metric, Time, Count, Con =  #ddb_connection{mode = normal}) ->
    case send_bin(dproto_tcp:encode({get, Bucket, Metric, Time, Count}), Con) of
        {ok, Con1 = #ddb_connection{socket = Socket}} ->
            case gen_tcp:recv(Socket, 0, ?TIMEOUT) of
                {ok, <<Resolution:64/integer, D/binary>>} ->
                    {ok, {Resolution, D}, Con1};
                {error, E} ->
                    {error, E, Con1}
            end;
        E ->
            E
    end;

get(_, _, _, _, _Con) ->
    {error, stream}.

%%--------------------------------------------------------------------
%% @doc Sends data to the server on streaming mode. Returns an error
%% when in stream mode.
%%
%% @spec send(Metric :: binary(),
%%            Time :: pos_integer(),
%%            Points :: [integer()] | binary(),
%%            Connection :: connection()) ->
%%         {ok, Connection :: connection()} |
%%         {error, Error :: inet:posix(), Connection :: connection()} |
%%         {error, no_stream}
%% @end
%%--------------------------------------------------------------------

send(Metric, Time, Points, Con =  #ddb_connection{mode = stream}) ->
    send_bin(dproto_tcp:stream({stream, Metric, Time, Points}), Con);

send(_, _, _, _Con) ->
    {error, no_stream}.

%%--------------------------------------------------------------------
%% @doc Forces to close a conneciton.
%%
%% @spec close(Connection :: connection()) ->
%%         Connection :: connection()
%% @end
%%--------------------------------------------------------------------
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

reconnect(Con = #ddb_connection{socket = undefined,
                                host = Host,
                                port = Port}) ->
    case gen_tcp:connect(Host, Port, ?OPTS) of
        {ok, Socket} ->
            reset_state(Con#ddb_connection{socket = Socket, error = none});
        {error, E} ->
            Con#ddb_connection{error = E}
    end;

reconnect(Con) ->
    Con.

reset_state(Con = #ddb_connection{socket = undefined}) ->
    Con;
reset_state(Con = #ddb_connection{socket = Socket, mode = stream}) ->
    gen_tcp:setopts(Socket, [{packet, 0}]),
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
