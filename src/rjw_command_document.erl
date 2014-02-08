
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

-module(rjw_command_document).

-export([
    handle/2
    ]).

-include("rjw_message.hrl").

handle(_, #insert{collection=_, documents=[]}) -> noreply;
handle(Db, #insert{collection=Collection, documents=[Doc|R]}=Command) ->
    {Key, JDocument} = rjw_util:bsondoc_to_json(Doc),

    riak_json:store_document(<<Db/binary, $.:8, Collection/binary>>, Key, JDocument),
    handle(Db, Command#insert{documents=R});
handle(_Db, #update{}=_Command) -> noreply;
handle(_Db, #delete{}=_Command) -> noreply.