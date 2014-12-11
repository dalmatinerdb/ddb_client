-module(ddb_client_connection).

-include_lib("dproto/include/dproto.hrl").
-define(TIMEOUT, 30000).

-export([
         connect/2,
         connect/1,
         mode/1,
         connected/1,
         disconnect/1,
         stream_mode/3,
         list/1,
         list/2,
         get/5,
         send/4,
         send/5
        ]).

-record(ddb_connection,
        {socket :: gen_tcp:socket() | undefined,
         host,
         port,
         mode = normal,
         bucket,
         error = none,
         delay = 1}).

-define(OPTS, [binary, {packet, 4}, {active, false}]).

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

connect(Con) ->
    reconnect(Con).

mode(#ddb_connection{mode = stream, bucket=Bucket}) ->
    {ok, {stream, Bucket}};
mode(#ddb_connection{mode = normal}) ->
    {ok, normal}.

connected(#ddb_connection{socket = undefined}) ->
    false;
connected(_) ->
    true.

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

list(Con =  #ddb_connection{mode = normal}) ->
    do_list(send_bin(<<?BUCKETS>>, Con));

list(Con) ->
    {error, stream, Con}.

list(Bucket, Con =  #ddb_connection{mode = normal}) ->
    do_list(send_bin(<<?LIST,
                       (byte_size(Bucket)):?BUCKET_SS/integer,
                       Bucket/binary>>, Con));

list(_Bucket, Con) ->
    {error, stream, Con}.

get(Bucket, Metric, Time, Count, Con =  #ddb_connection{mode = normal}) ->
    case send_bin(<<?GET,
                    (dproto_tcp:encode_get(Bucket, Metric, Time, Count))/binary>>,
                  Con) of
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

get(_, _, _, _, Con) ->
    {error, stream, Con}.



send(Bucket, Metric, Time, Points, Con =  #ddb_connection{mode = normal}) ->
    send_bin(<<?PUT,
               (dproto_udp:encode_header(Bucket))/binary,
               (dproto_udp:encode_points(Metric, Time, Points))/binary>>, Con);

send(_, _, _, _, Con) ->
    {error, stream, Con}.

send(Metric, Time, Points, Con =  #ddb_connection{mode = stream}) ->
    send_bin(dproto_tcp:encode_stream_payload(Metric, Time, Points), Con);

send(_, _, _, Con) ->
    {error, no_stream, Con}.

disconnect(Con = #ddb_connection{socket = undefined}) ->
    Con;
disconnect(Con = #ddb_connection{socket = Sock}) ->
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
            send1(Bin, reconnect(disconnect(Con)));
        _ ->
            {ok, Con}
    end.
send1(_Bin, Con = #ddb_connection{socket = undefined, error = E}) ->
    {error, E, Con};

send1(Bin, Con = #ddb_connection{socket = Sock}) ->
    case gen_tcp:send(Sock, Bin) of
        {error, E} ->
            {error, E, disconnect(Con)};
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
    en_tcp:setopts(Socket, [{packet, 0}]),
    Con;
reset_state(Con) ->
    Con.


decode_metrics(<<>>, Acc) ->
    Acc;

decode_metrics(<<S:16/integer, M:S/binary, R/binary>>, Acc) ->
    decode_metrics(R, [M | Acc]).

do_list({ok, Con1 = #ddb_connection{socket = S}}) ->
                case gen_tcp:recv(S, 0, ?TIMEOUT) of
                {ok, <<Size:32/integer, Reply:Size/binary>>} ->
                    {ok, decode_metrics(Reply, []), Con1};
                {error, E} ->
                    {error, E, Con1}
            end;
do_list(Error) ->
    Error.
