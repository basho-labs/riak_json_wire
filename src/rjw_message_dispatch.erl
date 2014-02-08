
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

-module(rjw_message_dispatch).
-export([
    send/2
    ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib ("rjw_message.hrl").

%%% =================================================== external api

%% admin commands
send(<<"admin">>, Command) -> rjw_command_admin:handle(Command);

%% schema commands
send(<<"schema:", Db/binary>>, Command) -> rjw_command_schema:handle(Db, Command);

%% document commands
send(Db, #insert{}=Command) -> rjw_command_document:handle(Db, Command);
send(Db, #update{}=Command) -> rjw_command_document:handle(Db, Command);
send(Db, #delete{}=Command) -> rjw_command_document:handle(Db, Command);

%% query commands
send(Db, #query{}=Command) -> rjw_command_query:handle(Db, Command);

%% unhandled commands
send(_, _) -> {error, undefined}.

%%% =================================================== tests


-ifdef(TEST).

ismaster_test() ->
    Input = rjw_message:put_message(admin, #query{collection = '$cmd', selector = {isMaster, 1}}, 1),
    {Db, Op, RequestId} = rjw_message:get_message(Input),

    ?assertEqual(1, RequestId),
    ?assertEqual(<<"admin">>, Db),

    Response = send(Db, Op),
                 
    %% ?assertEqual([{struct,[{isMaster,1}]}], Response#reply.documents).
    ?assertEqual([{ok,true,ismaster,1}], Response#reply.documents).

-endif.