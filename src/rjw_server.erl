
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

-module(rjw_server).
-behaviour(gen_server).
-behaviour(ranch_protocol).

-include("riak_json_wire.hrl").
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

-record(state, {
    socket,
    transport,
    socket_request_id = 0,
    session = #session{}
    }).

%%% =================================================== external api

start_link(Ref, Socket, Transport, Opts) ->
    proc_lib:start_link(?MODULE, init, [Ref, Socket, Transport, Opts]).

%% gen_server.

init([]) -> {ok, undefined}.

init(Ref, Socket, Transport, _Opts = []) ->
    ok = proc_lib:init_ack({ok, self()}),
    ok = ranch:accept_ack(Ref),
    ok = Transport:setopts(Socket, [{active, once}]),
    gen_server:enter_loop(?MODULE, [],
        #state{socket=Socket, transport=Transport},
        ?TIMEOUT).

handle_info({tcp, Socket, BsonPackets}, State=#state{
        socket=Socket}) ->

    NewState = get_responses(BsonPackets, State),

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

%% @doc Decodes Bson Packets
get_responses(BsonPackets, State) when is_binary(BsonPackets) ->
    {Messages, _} = rjw_message:get_messages(BsonPackets),
    get_responses(Messages, State);

%% @doc Builds and sends responses as they are processed
get_responses([], State) -> State;
get_responses([M|Messages], State) ->
    lager:debug("Message: ~p~n", [M]),
    Response = get_response(M, State),
    get_responses(Messages, increment_request(respond(Response, State))).

%% @doc Given message, creates Bson response
get_response({Db, Command, RequestId}, State) ->
    {ReplyType, Reply, Session} = riak_json_wire:dispatch_message(Db, Command, State#state.session),
    lager:debug("Response: ~p, ~p~n", [ReplyType, Reply]),
    BsonReply = rjw_message:put_reply(
        State#state.socket_request_id, 
        RequestId, 
        Reply
    ),

    {ReplyType, BsonReply, Session}.

%% @doc Sends a Bson reply if there is one, updates state
respond({noreply, _, Session}, State) ->
    State#state{session=Session};
respond({reply, Response, Session}, 
        State=#state{socket=Socket, transport=Transport}) when is_binary(Response) ->
    Transport:send(Socket, <<
        ?put_int32(byte_size(Response)+4),
        Response/binary
    >>),
    State#state{session=Session};
respond(Error, State) ->
    lager:error("Failed to identify response: ~p~n", [Error]),
    State.

increment_request(State) ->
    Current = State#state.socket_request_id,
    State#state{socket_request_id = Current + 1}.