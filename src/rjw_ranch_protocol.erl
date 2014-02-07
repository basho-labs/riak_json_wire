
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
    lager:debug("Ref: ~p, Socket: ~p, Transport: ~p~n", [Ref, Socket, Transport]),
    ok = proc_lib:init_ack({ok, self()}),
    ok = ranch:accept_ack(Ref),
    ok = Transport:setopts(Socket, [{active, once}]),
    gen_server:enter_loop(?MODULE, [],
        #state{socket=Socket, transport=Transport},
        ?TIMEOUT).

handle_info({tcp, Socket, Data}, State=#state{
        socket=Socket}) ->
    lager:debug("Data: ~p~n", [Data]),

    {Messages, _} = rjw_message:get_messages(Data),
    {ok, NewState} = respond(Messages, State),

    {noreply, NewState, ?TIMEOUT};
handle_info({tcp_closed, _Socket}, State) ->
    lager:debug("tcp_closed, State: ~p~n", [State]),
    {stop, normal, State};
handle_info({tcp_error, _, Reason}, State) ->
    lager:debug("tcp_error, State: ~p, Reason: ~n", [State, Reason]),
    {stop, Reason, State};
handle_info(timeout, State) ->
    lager:debug("timeout, State: ~p~n", [State]),
    {stop, normal, State};
handle_info(_Info, State) ->
    {stop, normal, State}.

handle_call({get_last_error}, _, State) ->
    {reply, State#state.last_error, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% =================================================== internal functions

error_bin(Str, Args) ->
    list_to_binary(lists:flatten(io_lib:format(Str, Args))).

respond([], State) -> {ok, State};

respond([M | R],
        State=#state{socket=Socket, transport=Transport}) ->
    Transport:setopts(Socket, [{active, once}]),

    {Db, Command, RequestId} = M,

    {Response, NewState} = case rjw_message_dispatch:send(Db,Command) of
        {error, undefined} -> 
            ErrMsg = error_bin("Unhandled message: ~p~n", [M]),
            lager:error("~p~n", [ErrMsg]),
            {#reply{documents = [{err, ErrMsg}]},
             State#state{last_error = ErrMsg}};
        {error, R} -> 
            ErrMsg = error_bin("Error occurred: ~p.", [R]),
            lager:error("~p~n", [ErrMsg]),
            {#reply{documents = [{err, ErrMsg}]},
             State#state{last_error = ErrMsg}};
        R -> {R, State}
    end,

    case Response of
        {noreply} -> ok;
        #reply{}=R ->
            BsonResponse = rjw_message:put_reply(State#state.socket_request_id, RequestId, R),
            Transport:send(Socket, <<
                ?put_int32(byte_size(BsonResponse)+4), 
                BsonResponse/binary
            >>);
        R -> lager:error("Failed to identify response: ~p~n", [R])
    end,

    NewSocketRequestId = State#state.socket_request_id + 1,

    respond(R, NewState#state{socket_request_id = NewSocketRequestId}).
