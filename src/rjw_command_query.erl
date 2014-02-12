
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

%%% =================================================== external api

% find (the last) one
handle(Db, #query{collection=Coll, batchsize=-1,selector={}}, Session) ->
    Key = rjw_server:get_last_insert(Db, Coll, Session),

    case riak_json:get_document(<<Db/binary, $.:8, Coll/binary>>, Key) of
        undefined -> 
            {#reply{documents = []}, Session};
        List ->
            JDocument = list_to_binary(List),
            {#reply{documents = rjw_util:json_to_bsondoc(Key, JDocument)}, Session}
    end;

% find one by id
handle(Db, #query{collection=Coll, selector={'_id',{Key}}}, Session) ->
    case riak_json:get_document(<<Db/binary, $.:8, Coll/binary>>, Key) of
        undefined -> 
            {#reply{documents = []}, Session};
        List ->
            JDocument = list_to_binary(List),
            {#reply{documents = rjw_util:json_to_bsondoc(Key, JDocument)}, Session}
    end;

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