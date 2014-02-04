-module(riak_json_wire_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    Port = app_helper:get_env(riak_json_wire, port, 27017),

    {ok, _} = ranch:start_listener(riak_json_wire, 10,
        ranch_tcp, [{port, Port}], rj_wire_protocol, []),

    riak_json_wire_sup:start_link().

stop(_State) ->
    ok.
