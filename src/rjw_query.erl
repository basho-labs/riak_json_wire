
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
    Key = riak_json_wire:get_last_insert(Db, Coll, Session),
    Docs = rjw_rj:get_document(Db, Coll, Key),
    {reply, #reply{documents = Docs}, Session};

% find one by id
handle(Db, #query{collection=Coll, selector={'_id',{BinKey}}, projector=Proj}, Session) ->
    %%TODO:Abstract key formation!
    Key = list_to_binary(rjw_util:bin_to_hexstr(BinKey)),
    case riak_json:get_document(<<Db/binary, $.:8, Coll/binary>>, Key) of
        undefined -> 
            {reply, #reply{documents = []}, Session};
        List ->
            JDocument = list_to_binary(List),
            KeysToInclude = case Proj of [] -> []; undefined -> []; _ -> proplists:get_keys(bson:fields(Proj)) end,
            Bsondoc = rjw_util:json_to_bsondoc(Key, JDocument, KeysToInclude),
            lager:debug("Bsondoc found: ~p~n", [Bsondoc]),
            {reply, #reply{documents = Bsondoc}, Session}
    end;

handle(Db, #query{collection=Coll, selector=Sel, projector=Proj}, Session) ->
    try
        {_, JDocument} = rjw_util:bsondoc_to_json(Sel),
        Document = mochijson2:decode(JDocument),
        SolrQuery = rj_query:from_json(Document, all),

        try
            {_, JsonResults} = riak_json:find(<<Db/binary, $.:8, Coll/binary>>, SolrQuery),
            lager:debug("Query: ~p~nResult: ~p~n", [SolrQuery, JsonResults]),
            
            JsonResponse = list_to_binary(rj_query_response:format_json_response(JsonResults, all, SolrQuery)),

            lager:debug("Formatted Result Bin: ~p~n", [JsonResponse]),

            Proplist = jsonx:decode(JsonResponse, [{format, proplist}]),
            %%TODO get pagination stuff from this: % <<"{\"total\":2,\"page\":0,\"per_page\":100,\"num_pages\":1,\"data\":[{\"_id\":\"52fb05d2b1297d1b18000003\",\"i\":30},{\"_id\":\"52fb0686b1297d24cd000003\",\"i\":30}]}">>
            Data = proplists:get_value(<<"data">>, Proplist),
            KeysToInclude = case Proj of [] -> []; undefined -> []; _ -> proplists:get_keys(bson:fields(Proj)) end,
            Docs = [rjw_util:proplist_to_doclist(X, KeysToInclude, [])|| X <- Data],
            {reply, #reply{documents = Docs}, Session}
        catch
            Exception1:Reason1 ->
                lager:error("Query failed, ~p: ~p:~p, ~p", [SolrQuery, Exception1, Reason1, erlang:get_stacktrace()]), 
                {reply, #reply{documents = [{ok, false, err, <<"Query failed.">>}]}, Session}
        end
    catch
        Exception:Reason ->
            lager:debug("Malformed query, ~p: ~p:~p, ~p", [Sel, Exception, Reason, erlang:get_stacktrace()]), 
            {reply, #reply{documents = [{ok, false, err, <<"Malformed query.">>}]}, Session}
    end;

% regex
% {<<"testdb">>,{query,false,false,false,false,<<"testCollection">>,0,0,{name,{regex,<<"^M">>,<<>>}},[]},4}

handle(Db, Command, Session) -> 
    lager:error("Unhandled Command: ~p on Db: ~n", [Command, Db]),
    {reply, #reply{documents = [{ok, false, err, <<"Operation not supported.">>}]}, Session}.