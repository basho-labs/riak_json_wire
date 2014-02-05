
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

%% @doc riak_mongo_wire external api

-module(riak_json_wire).
-export([
    process_bson/1
    ]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib ("rj_wire_message.hrl").

process_bson(_Bson) ->
    ok.
    % {Messages, Rest} = riak_mongo_protocol:decode_wire(Bson),
    % ?debugFmt("Rest: ~p~n", [Rest]),
    % dispatch_request(Messages).

% create_bson(R, Reply) ->
%     Message = Reply#mongo_reply{ request_id = 1, reply_to = element(2, R)},

%     error_logger:info_msg("replying ~p~n", [Message]),

%     {ok, Packet} = riak_mongo_protocol:encode_packet(Message),
%     Packet.

% %% Internal
% dispatch_request([]) ->
%     lager:debug("No More Messages~n", []),
%     ok;

% dispatch_request([R|T]) ->
%     lager:debug("Message: ~p~n", [R]),
%     ?debugFmt("Message: ~p~n", [R]),

%     Reply = case R of
%         #mongo_query{}=M -> #mongo_reply{ documents=[ {struct, []} ]};
%         #mongo_getmore{}=M -> ok;
%         #mongo_killcursor{}=M -> ok;
%         #mongo_insert{}=M -> ok;
%         #mongo_delete{}=M -> ok;
%         #mongo_update{}=M -> ok;
%         _ -> lager:error("Unhandled Message: ~p~n", [R])
%     end,

%     Bson = create_bson(R, Reply),

%     ?debugFmt("Bson: ~p~n", [Bson]),

%     dispatch_request(T).

% %% Tests

% -ifdef(TEST).

% connection_test() ->
%     Bson = <<58,0,0,0,1,0,0,0,0,0,0,0,212,7,0,0,0,0,0,0,97,100,109,105,110,46,36,99,109,100,0,0,0,0,0,255,255,255,255,19,0,0,0,16,105,115,109,97,115,116,101,114,0,1,0,0,0,0>>,

%     Result = process_bson(Bson),

%     ?assertEqual(true, Result).

% -endif.