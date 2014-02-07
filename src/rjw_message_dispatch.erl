
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
send(<<"admin">>, Command) ->
    rjw_command_admin:handle(Command);

send(Db, #insert{}) -> {error, undefined};

%% TODO move to admin or query dispatch?
send(Db, #query{selector= <<"$cmd">>}=Command) ->
    Docs = [{ok, true, err, gen_server:call(rjw_ranch_protocol, {getlasterror})}],
    #reply{documents = Docs};

% 2014-02-06 17:45:59.341 [error] <0.2544.0>@rjw_message_dispatch:send:41 Undefined Message: {<<"testdb">>,{insert,<<"testCollection">>,[{'_id',<<82,244,16,167,177,41,125,7,81,0,0,1>>,name,<<"MongoDB">>,type,<<"database">>,count,1,info,{x,203,y,<<"102">>}}]},4}
% 2014-02-06 17:45:59.341 [error] <0.2544.0>@rjw_ranch_protocol:respond:106 Unhandled Message: {<<"testdb">>,{insert,<<"testCollection">>,[{'_id',<<82,244,16,167,177,41,125,7,81,0,0,1>>,name,<<"MongoDB">>,type,<<"database">>,count,1,info,{x,203,y,<<"102">>}}]},4}
% 2014-02-06 17:45:59.341 [error] <0.2544.0>@rjw_message_dispatch:send:41 Undefined Message: {<<"testdb">>,{query,false,false,false,false,<<"$cmd">>,0,-1,{getlasterror,1,j,false,fsync,false,wtimeout,null},[]},5}
% 2014-02-06 17:45:59.341 [error] <0.2544.0>@rjw_ranch_protocol:respond:106 Unhandled Message: {<<"testdb">>,{query,false,false,false,false,<<"$cmd">>,0,-1,{getlasterror,1,j,false,fsync,false,wtimeout,null},[]},5}

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