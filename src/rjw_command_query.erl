
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

-module(rjw_command_query).

-export([
    handle/3
    ]).

-include("rjw_message.hrl").

% find (the last) one
handle(Db, #query{collection=Coll, batchsize=-1,selector={}}, Session) ->
    Key = rjw_server:get_last_insert(Db, Coll, Session),
    JDocument = list_to_binary(riak_json:get_document(<<Db/binary, $.:8, Coll/binary>>, Key)),
    {#reply{documents = [rjw_util:json_to_bsondoc(Key, JDocument)]}, Session};

% find one by id
handle(Db, #query{collection=Coll, selector={'_id',{Key}}}, Session) ->
    JDocument = list_to_binary(riak_json:get_document(<<Db/binary, $.:8, Coll/binary>>, Key)),
    {#reply{documents = [rjw_util:json_to_bsondoc(Key, JDocument)]}, Session};

% find one by i field
% {<<"testdb">>,{query,false,false,false,false,<<"testCollection">>,0,0,{i,71},[]},4}

    % get_objects(BucketKeyList) %[{Bucket, Key}]
% find all and create a cursor
% {<<"testdb">>,{query,false,false,false,false,<<"testCollection">>,0,0,{},[]},4}

% get a count of collection entries
% {<<"testdb">>,{query,false,false,false,false,<<"$cmd">>,0,-1,{count,<<"testCollection">>,query,{},fields,undefined},[]},4}

% query operators
% {<<"testdb">>,{query,false,false,false,false,<<"testCollection">>,0,0,{i,{'$gt',20,'$lte',30}},[]},4}

% field select
% {<<"testdb">>,{query,false,false,false,false,<<"testCollection">>,0,0,{'_id',{<<82,245,204,189,177,41,125,120,154,0,0,1>>}},{name,1,type,1}},4}

% regex
% {<<"testdb">>,{query,false,false,false,false,<<"testCollection">>,0,0,{name,{regex,<<"^M">>,<<>>}},[]},4}

handle(Db, Command, Session) -> 
    lager:error("Unhandled Command: ~p on Db: ~n", [Command, Db]),
    {#reply{documents = [{ok, false, err, <<"Operation not supported.">>}]}, Session}.






%just need to convert to bsondoc

% 2014-02-09 22:06:57.419 [info] <0.350.0>@riak_core:wait_for_service:487 Waiting for service riak_kv to start (0 seconds)
% 2014-02-09 22:06:58.354 [info] <0.7.0> Application riak_json_http started on node 'riak@127.0.0.1'
% 2014-02-09 22:06:58.487 [debug] <0.583.0> Supervisor {<0.583.0>,ranch_listener_sup} started ranch_conns_sup:start_link(riak_json_wire, worker, 5000, ranch_tcp, 5000, rjw_server) at pid <0.584.0>
% 2014-02-09 22:07:07.547 [info] <0.350.0>@riak_core:wait_for_service:481 Wait complete for service riak_kv (10 seconds)
% 2014-02-09 22:07:14.181 [debug] <0.2724.0>@rjw_server:respond:142 Message: {<<"admin">>,{query,false,false,false,false,<<"$cmd">>,0,-1,{ismaster,1},[]},1}, Session: {session,[],[]}
% 2014-02-09 22:07:14.219 [debug] <0.2736.0>@rjw_server:respond:142 Message: {<<"testdb">>,{insert,<<"testCollection1">>,[{'_id',{<<82,248,66,98,253,100,217,215,33,0,0,1>>},name,<<"MongoDB">>,type,<<"database">>,count,1}]},2}, Session: {session,[],[]}
% 2014-02-09 22:07:14.272 [debug] <0.2736.0>@rj_yz:index_exists:108 Collection: <<"testdb.testCollection1">>
% 2014-02-09 22:07:14.294 [debug] <0.2736.0>@rj_yz:index_exists:124 Result: true
% 2014-02-09 22:07:14.294 [debug] <0.2736.0>@rj_yz:put:56 Collection: <<"testdb.testCollection1">>, Key: <<82,248,66,98,253,100,217,215,33,0,0,1>>, Doc: <<"{\"name\":\"MongoDB\",\"type\":\"database\",\"count\":1}">>
% 2014-02-09 22:07:14.537 [debug] <0.2736.0>@rj_yz:put:65 Result: ok
% 2014-02-09 22:07:14.537 [debug] <0.2736.0>@rjw_server:respond:142 Message: {<<"testdb">>,{query,false,false,false,false,<<"$cmd">>,0,-1,{getlasterror,1,j,false,fsync,false,wtimeout,undefined},[]},3}, Session: {session,[],[{<<"testdb.testCollection1">>,<<82,248,66,98,253,100,217,215,33,0,0,1>>}]}
% 2014-02-09 22:07:14.539 [debug] <0.2736.0>@rjw_server:respond:142 Message: {<<"testdb">>,{query,false,false,false,false,<<"system.namespaces">>,0,0,{},[]},4}, Session: {session,[],[{<<"testdb.testCollection1">>,<<82,248,66,98,253,100,217,215,33,0,0,1>>}]}
% 2014-02-09 22:07:14.539 [debug] <0.2736.0>@rjw_command_admin:handle:160 Types: [{name,<<"testdb.testCollection2RJType">>},{name,<<"testdb.testCollection1RJType">>},{name,<<"testdb.testCollection3RJType">>},{name,<<"testdb1.testCollection1RJType">>}]
% 2014-02-09 22:07:14.540 [debug] <0.2736.0>@rjw_server:respond:142 Message: {<<"testdb1">>,{query,false,false,false,false,<<"system.namespaces">>,0,0,{},[]},5}, Session: {session,[],[{<<"testdb.testCollection1">>,<<82,248,66,98,253,100,217,215,33,0,0,1>>}]}
% 2014-02-09 22:07:14.540 [debug] <0.2736.0>@rjw_command_admin:handle:160 Types: [{name,<<"testdb1.testCollection1RJType">>}]
% 2014-02-09 22:07:14.541 [debug] <0.2736.0>@rjw_server:respond:142 Message: {<<"testdb">>,{query,false,false,false,false,<<"testCollection1">>,0,-1,{},[]},6}, Session: {session,[],[{<<"testdb.testCollection1">>,<<82,248,66,98,253,100,217,215,33,0,0,1>>}]}
% 2014-02-09 22:07:14.548 [debug] <0.2736.0>@rj_yz:get:40 Collection: <<"testdb.testCollection1">>, Key: <<82,248,66,98,253,100,217,215,33,0,0,1>>
% 2014-02-09 22:07:14.566 [debug] <0.2736.0>@rj_yz:get:47 Result: {value,<<"{\"name\":\"MongoDB\",\"type\":\"database\",\"count\":1}">>}
% 2014-02-09 22:07:14.568 [error] <0.584.0> Ranch listener riak_json_wire had connection process started with rjw_server:start_link/4 at <0.2736.0> exit with reason: {badarg,[{erlang,tuple_size,[<<"{\"name\":\"MongoDB\",\"type\":\"database\",\"count\":1}">>],[]},{bson,doc_foldl,3,[{file,"src/bson.erl"},{line,31}]},{bson_binary,put_document,1,[{file,"src/bson_binary.erl"},{line,91}]},{rjw_message,'-put_reply/3-lbc$^0/2-0-',2,[{file,"src/rjw_message.erl"},{line,135}]},{rjw_message,put_reply,3,[{file,"src/rjw_message.erl"},{line,135}]},{rjw_server,respond_tcp,3,[{file,"src/rjw_server.erl"},{line,127}]},{rjw_server,respond,2,[{file,"src/rjw_server.erl"},{line,156}]},{rjw_server,handle_info,2,[{file,"src/rjw_server.erl"},{line,99}]}]}
% 2014-02-09 22:07:14.571 [debug] <0.2771.0>@rjw_server:respond:142 Message: {<<"testdb">>,{query,false,false,false,false,<<"testCollection1">>,0,-1,{},[]},7}, Session: {session,[],[]}
% 2014-02-09 22:07:14.571 [debug] <0.2771.0>@rj_yz:get:40 Collection: <<"testdb.testCollection1">>, Key: []
% 2014-02-09 22:07:14.572 [debug] <0.2771.0>@rj_yz:get:47 Result: {error,notfound}
% 2014-02-09 22:07:14.572 [error] <0.2771.0> gen_server <0.2771.0> terminated with reason: bad argument in call to erlang:list_to_binary(undefined) in rjw_command_query:handle/3 line 37
% 2014-02-09 22:07:14.573 [error] <0.2771.0> CRASH REPORT Process <0.2771.0> with 0 neighbours exited with reason: bad argument in call to erlang:list_to_binary(undefined) in rjw_command_query:handle/3 line 37 in gen_server:terminate/6 line 744
% 2014-02-09 22:07:14.573 [error] <0.584.0> Ranch listener riak_json_wire had connection process started with rjw_server:start_link/4 at <0.2771.0> exit with reason: {badarg,[{erlang,list_to_binary,[undefined],[]},{rjw_command_query,handle,3,[{file,"src/rjw_command_query.erl"},{line,37}]},{rjw_server,respond,2,[{file,"src/rjw_server.erl"},{line,144}]},{rjw_server,handle_info,2,[{file,"src/rjw_server.erl"},{line,99}]},{gen_server,handle_msg,5,[{file,"gen_server.erl"},{line,604}]},{proc_lib,init_p_do_apply,3,[{file,"proc_lib.erl"},{line,239}]}]}
% 2014-02-09 22:07:14.574 [debug] <0.2774.0>@rjw_server:respond:142 Message: {<<"testdb">>,{query,false,false,false,false,<<"testCollection1">>,0,-1,{},[]},8}, Session: {session,[],[]}
% 2014-02-09 22:07:14.574 [debug] <0.2774.0>@rj_yz:get:40 Collection: <<"testdb.testCollection1">>, Key: []
% 2014-02-09 22:07:14.575 [debug] <0.2774.0>@rj_yz:get:47 Result: {error,notfound}
% 2014-02-09 22:07:14.576 [error] <0.2774.0> gen_server <0.2774.0> terminated with reason: bad argument in call to erlang:list_to_binary(undefined) in rjw_command_query:handle/3 line 37
% 2014-02-09 22:07:14.576 [error] <0.2774.0> CRASH REPORT Process <0.2774.0> with 0 neighbours exited with reason: bad argument in call to erlang:list_to_binary(undefined) in rjw_command_query:handle/3 line 37 in gen_server:terminate/6 line 744
% 2014-02-09 22:07:14.577 [error] <0.584.0> Ranch listener riak_json_wire had connection process started with rjw_server:start_link/4 at <0.2774.0> exit with reason: {badarg,[{erlang,list_to_binary,[undefined],[]},{rjw_command_query,handle,3,[{file,"src/rjw_command_query.erl"},{line,37}]},{rjw_server,respond,2,[{file,"src/rjw_server.erl"},{line,144}]},{rjw_server,handle_info,2,[{file,"src/rjw_server.erl"},{line,99}]},{gen_server,handle_msg,5,[{file,"gen_server.erl"},{line,604}]},{proc_lib,init_p_do_apply,3,[{file,"proc_lib.erl"},{line,239}]}]}
% 2014-02-09 22:07:14.578 [debug] <0.2781.0>@rjw_server:respond:142 Message: {<<"testdb">>,{query,false,false,false,false,<<"testCollection1">>,0,-1,{},[]},9}, Session: {session,[],[]}
% 2014-02-09 22:07:14.578 [debug] <0.2781.0>@rj_yz:get:40 Collection: <<"testdb.testCollection1">>, Key: []
% 2014-02-09 22:07:14.579 [debug] <0.2781.0>@rj_yz:get:47 Result: {error,notfound}
% 2014-02-09 22:07:14.579 [error] <0.2781.0> gen_server <0.2781.0> terminated with reason: bad argument in call to erlang:list_to_binary(undefined) in rjw_command_query:handle/3 line 37
% 2014-02-09 22:07:14.580 [error] <0.2781.0> CRASH REPORT Process <0.2781.0> with 0 neighbours exited with reason: bad argument in call to erlang:list_to_binary(undefined) in rjw_command_query:handle/3 line 37 in gen_server:terminate/6 line 744
% 2014-02-09 22:07:14.580 [error] <0.584.0> Ranch listener riak_json_wire had connection process started with rjw_server:start_link/4 at <0.2781.0> exit with reason: {badarg,[{erlang,list_to_binary,[undefined],[]},{rjw_command_query,handle,3,[{file,"src/rjw_command_query.erl"},{line,37}]},{rjw_server,respond,2,[{file,"src/rjw_server.erl"},{line,144}]},{rjw_server,handle_info,2,[{file,"src/rjw_server.erl"},{line,99}]},{gen_server,handle_msg,5,[{file,"gen_server.erl"},{line,604}]},{proc_lib,init_p_do_apply,3,[{file,"proc_lib.erl"},{line,239}]}]}

