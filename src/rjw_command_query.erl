
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

handle(_Db, #query{}=_Command, Session) -> {#reply{documents = [{ok, true}]}, Session}.

% jsonx:decode(<<"{\"name\":\"Ivan\",\"age\":33,\"phones\":[3332211,4443322]}">>, [{format, proplist}]).
% [{<<"name">>,<<"Ivan">>},
%  {<<"age">>,33},
%  {<<"phones">>,[3332211,4443322]}]

% find (the last) one
% {<<"testdb">>,{query,false,false,false,false,<<"testCollection">>,0,-1,{},[]},4}

% find one by id
% {<<"testdb">>,{query,false,false,false,false,<<"testCollection">>,0,0,{'_id',{<<82,245,202,253,177,41,125,100,219,0,0,1>>}},[]},4}

% find one by i field
% {<<"testdb">>,{query,false,false,false,false,<<"testCollection">>,0,0,{i,71},[]},4}

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