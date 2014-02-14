
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

-module(rjw_schema).

-export([
    handle/3
    ]).

-include("riak_json_wire.hrl").

handle(Db, #query{collection=Coll, batchsize=-1,selector={}}, Session) ->
    Docs = rjw_rj:get_schema(Db, Coll),
    {reply, #reply{documents = Docs}, Session};

handle(_Db, #insert{}=_Command, Session) -> {noreply, undefined, Session};
handle(_Db, #update{}=_Command, Session) -> {noreply, undefined, Session};
handle(_Db, #delete{}=_Command, Session) -> {noreply, undefined, Session}.