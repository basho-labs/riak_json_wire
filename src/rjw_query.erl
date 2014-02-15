
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

-module(rjw_query).

-export([
    handle/3
    ]).

-include("riak_json_wire.hrl").

%%% =================================================== external api

% find (the last) one
handle(Db, #query{collection=Coll, batchsize=-1,selector={}}, Session) ->
    Id = riak_json_wire:get_last_insert(Db, Coll, Session),
    Docs = rjw_rj:get_document(Db, Coll, Id, []),
    {reply, #reply{documents = Docs}, Session};

% find one by id
handle(Db, #query{collection=Coll, selector={'_id',{Id}}, projector=Proj}, Session) ->
    Docs = rjw_rj:get_document(Db, Coll, Id, Proj),
    {reply, #reply{documents = Docs}, Session};

handle(Db, #query{collection=Coll, selector=Sel, projector=Proj}, Session) ->
    Docs = rjw_rj:find(Db, Coll, Sel, Proj),
    {reply, #reply{documents = Docs}, Session};

handle(Db, Command, Session) -> 
    lager:error("Unhandled Command: ~p on Db: ~n", [Command, Db]),
    {reply, #reply{documents = [{ok, false, err, <<"Operation not supported.">>}]}, Session}.