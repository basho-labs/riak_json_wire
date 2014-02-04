-module(rj_wire_protocol).
-behaviour(gen_server).
-behaviour(ranch_protocol).

%% API.
-export([start_link/4]).

%% gen_server.
-export([init/1]).
-export([init/4]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-define(TIMEOUT, 60000).

-record(state, {socket, transport}).

%% API.

start_link(Ref, Socket, Transport, Opts) ->
	proc_lib:start_link(?MODULE, init, [Ref, Socket, Transport, Opts]).

%% gen_server.

%% This function is never called. We only define it so that
%% we can use the -behaviour(gen_server) attribute.
init([]) -> {ok, undefined}.


% 2014-02-04 03:11:30.700 [debug] <0.4016.0>@rj_wire_protocol:init:33 Ref: riak_json_wire, Socket: #Port<0.12000>, Transport: ranch_tcp
% 2014-02-04 03:11:30.701 [debug] <0.4016.0>@rj_wire_protocol:handle_info:44 Data: <<58,0,0,0,1,0,0,0,0,0,0,0,212,7,0,0,0,0,0,0,97,100,109,105,110,46,36,99,109,100,0,0,0,0,0,255,255,255,255,19,0,0,0,16,105,115,109,97,115,116,101,114,0,1,0,0,0,0>>

init(Ref, Socket, Transport, _Opts = []) ->
	lager:debug("Ref: ~p, Socket: ~p, Transport: ~p~n", [Ref, Socket, Transport]),
	ok = proc_lib:init_ack({ok, self()}),
	ok = ranch:accept_ack(Ref),
	ok = Transport:setopts(Socket, [{active, once}]),
	gen_server:enter_loop(?MODULE, [],
		#state{socket=Socket, transport=Transport},
		?TIMEOUT).

handle_info({tcp, Socket, Data}, State=#state{
		socket=Socket, transport=Transport}) ->
	Transport:setopts(Socket, [{active, once}]),
	lager:debug("Data: ~p~n", [Data]),
	Transport:send(Socket, Data),
	{noreply, State, ?TIMEOUT};
handle_info({tcp_closed, _Socket}, State) ->
	{stop, normal, State};
handle_info({tcp_error, _, Reason}, State) ->
	{stop, Reason, State};
handle_info(timeout, State) ->
	{stop, normal, State};
handle_info(_Info, State) ->
	{stop, normal, State}.

handle_call(_Request, _From, State) ->
	{reply, ok, State}.

handle_cast(_Msg, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% Internal.
