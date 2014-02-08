
%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(rjw_ranch_protocol).
-behaviour(gen_server).
-behaviour(ranch_protocol).

-include("rjw_message.hrl").
-include_lib("bson/include/bson_binary.hrl").

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

-record(state, {socket, transport, socket_request_id = 0, last_error = <<>>}).

%%% =================================================== external api

start_link(Ref, Socket, Transport, Opts) ->
    proc_lib:start_link(?MODULE, init, [Ref, Socket, Transport, Opts]).

%% gen_server.

%% This function is never called. We only define it so that
%% we can use the -behaviour(gen_server) attribute.
init([]) -> {ok, undefined}.

init(Ref, Socket, Transport, _Opts = []) ->
    ok = proc_lib:init_ack({ok, self()}),
    ok = ranch:accept_ack(Ref),
    ok = Transport:setopts(Socket, [{active, once}]),
    gen_server:enter_loop(?MODULE, [],
        #state{socket=Socket, transport=Transport},
        ?TIMEOUT).

handle_info({tcp, Socket, Data}, State=#state{
        socket=Socket}) ->

    {Messages, _} = rjw_message:get_messages(Data),
    {ok, NewState} = respond(Messages, State),

    {noreply, NewState, ?TIMEOUT};
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

%%% =================================================== internal functions

update_state(ok, _, State) -> State;
update_state(Errors, M, State) ->
    lager:error("Errors: ~p, Message: ~p~n", [Errors, M]),
    State#state{last_error = Errors}.

respond_tcp(Reply, RequestId, State=#state{socket=Socket, transport=Transport}) ->
    Transport:setopts(Socket, [{active, once}]),
    BsonResponse = rjw_message:put_reply(
        State#state.socket_request_id, 
        RequestId, Reply
    ),
    Transport:send(Socket, <<
        ?put_int32(byte_size(BsonResponse)+4),
        BsonResponse/binary
    >>).

respond([], State) -> {ok, State};
%% TODO move to admin or query dispatch?
respond([{_, ?getlasterror(_J,_FS,_WT), RequestId} = M | R], State=#state{last_error = E}) ->
    lager:debug("Message: ~p~n", [M]),
    Docs = case E of
        <<>> -> [{ok, true, err, null}];
        Error -> [{ok, true, err, Error}]
    end,

    respond_tcp(#reply{documents=Docs}, RequestId, State#state{last_error = <<>>}),

    NewSocketRequestId = State#state.socket_request_id + 1,
    respond(R, State#state{socket_request_id = NewSocketRequestId});
respond([M | R], State) ->
    lager:debug("Message: ~p~n", [M]),
    {Db, Command, RequestId} = M,

    Errors = case rjw_message_dispatch:send(Db,Command) of
        {error, undefinedreply} -> 
            Reply = #reply{documents = [{ok, false, err, <<"Operation not supported.">>}]},
            respond_tcp(Reply, RequestId, State), <<"Operation not supported.">>;
        {error, undefined} -> <<"Operation not supported.">>;
        {error, Reason} -> Reason;
        noreply -> ok;
        #reply{}=Reply -> respond_tcp(Reply, RequestId, State), ok;
        Reply -> lager:error("Failed to identify response: ~p~n", [Reply]), ok
    end,

    NewState = update_state(Errors, M, State),
    NewSocketRequestId = State#state.socket_request_id + 1,
    respond(R, NewState#state{socket_request_id = NewSocketRequestId}).
