-module(riak_json_wire_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case rj_wire_config:is_enabled() of
        true ->
            ensure_started(ranch),

            Port = rj_wire_config:port(),

            {ok, _} = ranch:start_listener(riak_json_wire, 10,
            ranch_tcp, [{port, Port}], rj_wire_protocol, []);
        _ -> ok
    end,

    riak_json_wire_sup:start_link().

stop(_State) ->
    ok.

ensure_started(App) ->
    case application:start(App) of
        ok ->
            ok;
        {error, {already_started, App}} ->
            ok
    end.