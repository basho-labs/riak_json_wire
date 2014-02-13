
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

-module(riak_json_wire).
-export([
    dispatch_message/3,
    set_last_error/3,
    get_last_error/2,
    append_last_insert/4,
    set_last_insert/4,
    get_last_insert/3
    ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib ("riak_json_wire.hrl").

%%% =================================================== external api

set_last_error(Db, E, Session) ->
    NewErrors = lists:keystore(Db, 1, Session#session.last_errors, {Db, E}),
    Session#session{last_errors = NewErrors}.
get_last_error(Db, Session) -> 
    proplists:get_value(Db, Session#session.last_errors, <<>>).

append_last_insert(Db, Coll, Insert, Session) ->
    NewInserts = Session#session.last_inserts ++ [{<<Db/binary, $.:8, Coll/binary>>, Insert}],
    Session#session{last_inserts = NewInserts}.
set_last_insert(Db, Coll, <<>>, Session) ->
    NewInserts = proplists:delete(<<Db/binary, $.:8, Coll/binary>>, Session#session.last_inserts),
    Session#session{last_inserts = NewInserts};
set_last_insert(Db, Coll, Insert, Session) ->
    DbColl = <<Db/binary, $.:8, Coll/binary>>,
    NewInserts = lists:keystore(DbColl, 1, Session#session.last_inserts, {DbColl, Insert}),
    Session#session{last_inserts = NewInserts}.
get_last_insert(Db, Coll, Session) ->
    proplists:get_value(<<Db/binary, $.:8, Coll/binary>>, Session#session.last_inserts, []).

%% admin commands
dispatch_message(<<"admin">>=Db, Command, Session) -> 
    rjw_admin:handle(Db, Command, Session);
dispatch_message(Db, #query{collection= <<"system.", _/binary>>}=Command, Session) -> 
    rjw_admin:handle(Db, Command, Session);
dispatch_message(Db, #query{collection= <<"$cmd">>}=Command, Session) -> 
    rjw_admin:handle(Db, Command, Session);

%% schema commands
dispatch_message(<<"schema:", Db/binary>>, Command, Session) -> rjw_schema:handle(Db, Command, Session);

%% document commands
dispatch_message(Db, #insert{}=Command, Session) -> 
    NewSession = set_last_insert(Db, Command#insert.collection, <<>>, Session),
    rjw_document:handle(Db, Command, NewSession);
dispatch_message(Db, #update{}=Command, Session) -> rjw_document:handle(Db, Command, Session);
dispatch_message(Db, #delete{}=Command, Session) -> rjw_document:handle(Db, Command, Session);

%% query commands
dispatch_message(Db, #query{}=Command, Session) -> rjw_query:handle(Db, Command, Session);

%% unhandled commands
dispatch_message(Db, Command, Session) -> 
    lager:error("Unhandled Command: ~p on Db: ~n", [Command, Db]),
    {noreply, undefined, set_last_error(Db, <<"Operation not supported.">>, Session)}.

%%% =================================================== internal functions

%%% =================================================== tests

-ifdef(TEST).

ismaster_test() ->
    Input = rjw_message:put_message(admin, #query{collection = '$cmd', selector = {isMaster, 1}}, 1),
    {Db, Op, RequestId} = rjw_message:get_message(Input),

    ?assertEqual(1, RequestId),
    ?assertEqual(<<"admin">>, Db),

    {reply, Response, _} = dispatch_message(Db, Op, #session{}),

    ?assertEqual([{ok,true,ismaster,1}], Response#reply.documents).

-endif.