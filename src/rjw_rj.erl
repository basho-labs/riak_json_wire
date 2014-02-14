
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

-module(rjw_rj).

-export([
    get_schema/2
    ]).

-include_lib("bson/include/bson_binary.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%%% =================================================== external api

get_schema(Db, Coll) ->
    case riak_json:get_default_schema(dbcoll(Db, Coll)) of
        {error, _} -> [];
        List -> {fields, json_to_bsondocs(List)}
    end.

get_document(Db, Coll, Key) ->
    case riak_json:get_document(dbcoll(Db, Coll), Key) of
        undefined -> [];
        List -> rjw_util:json_to_bsondoc(Key, List)
    end.

%%% =================================================== internal functions

dbcoll(Db, Coll) -> <<Db/binary, $.:8, Coll/binary>>.

json_to_bsondocs(AnyJDocument) ->
    JDocument = rj_util:any_to_binary(AnyJDocument),
    Proplist = jsonx:decode(JDocument, [{format, proplist}]),
    [rjw_util:proplist_to_doclist(X, []) || X <- Proplist].

json_to_bsondoc(Key, Json) ->
    json_to_bsondoc(Key, Json, []).

json_to_bsondoc(Key, AnyJDocument, KeysToInclude) ->
    JDocument = rj_util:any_to_binary(AnyJDocument),
    Proplist = jsonx:decode(JDocument, [{format, proplist}]),

    WithId = case Key of
        undefined -> Proplist;
        K -> [{'_id', {hexstr_to_bin(rj_util:any_to_binary(K))}} | Proplist]
    end,

    proplist_to_doclist(WithId, KeysToInclude, []).

proplist_to_doclist(Proplist, KeysToInclude, []) ->
    CorrectKeys = case KeysToInclude of
        [] -> Proplist;
        _ -> 
            C = [ {list_to_binary(atom_to_list(K)), proplists:get_value(list_to_binary(atom_to_list(K)), Proplist)} || K <- KeysToInclude ],
            case proplists:get_value('_id', Proplist) of
                undefined -> C;
                K -> [{'_id', K} | C]
            end
    end,

    proplist_to_doclist(CorrectKeys, []).

proplist_to_doclist([], Doclist) ->
    bson:document(lists:reverse(Doclist));
proplist_to_doclist([{K, Doc} | R], Doclist) when is_tuple(Doc) ->
    proplist_to_doclist(R, [{K, Doc} | Doclist]);
proplist_to_doclist([{K, Doc} | R], Doclist) when is_list(Doc) ->
    proplist_to_doclist(R, [{K, proplist_to_doclist(Doc, [])} | Doclist]);
proplist_to_doclist([{K, Doc} | R], Doclist)->
    proplist_to_doclist(R, [{K, Doc} | Doclist]).

bsondoc_to_json(Doc) ->
    DocList = bson:fields(Doc),
    {Key, WithoutId} = case proplists:get_value('_id', DocList) of
        undefined -> {undefined, DocList};
        {K} -> {list_to_binary(bin_to_hexstr(K)), proplists:delete('_id', DocList)}
    end,
    {Key, jsonx:encode(doclist_to_proplist(WithoutId, []))}.

doclist_to_proplist([], Doclist) ->
    lists:reverse(Doclist);
doclist_to_proplist([{K, Doc} | R], Doclist) when is_tuple(Doc) ->
    doclist_to_proplist(R, [{K, doclist_to_proplist(bson:fields(Doc), [])} | Doclist]);
doclist_to_proplist([{K, Doc} | R], Doclist) when is_list(Doc) ->
    doclist_to_proplist(R, [{K, doclist_to_proplist(Doc, [])} | Doclist]);
doclist_to_proplist([{K, Doc} | R], Doclist)->
    doclist_to_proplist(R, [{K, Doc} | Doclist]).

proplist_replaceall([], NewList) -> NewList;
proplist_replaceall([{Key,_}=NewTuple|R], NewList) ->
    proplist_replaceall(R, lists:keystore(Key, 1, NewList, NewTuple)).

hex(N) when N < 10 ->
    $0+N;
hex(N) when N >= 10, N < 16 ->
    $a+(N-10).

int(C) when $0 =< C, C =< $9 ->
    C - $0;
int(C) when $A =< C, C =< $F ->
    C - $A + 10;
int(C) when $a =< C, C =< $f ->
    C - $a + 10.
    
to_hex(N) when N < 256 ->
    [hex(N div 16), hex(N rem 16)].
 
list_to_hexstr([]) -> 
    [];
list_to_hexstr([H|T]) ->
    to_hex(H) ++ list_to_hexstr(T).

bin_to_hexstr(Bin) ->
    list_to_hexstr(binary_to_list(Bin)).

hexstr_to_bin(S) ->
    list_to_binary(hexstr_to_list(S)).

hexstr_to_list([X,Y|T]) ->
    [int(X)*16 + int(Y) | hexstr_to_list(T)];
hexstr_to_list([]) ->
    [].


-ifdef(TEST).

bsondoc_test() ->
    Input = {'_id',{<<82,245,142,32,177,41,125,173,127,0,0,1>>},name,<<"MongoDB">>,type,<<"database">>,count,1,info,{x,203,y,<<"102">>}},
    Expected = {<<"52f58e20b1297dad7f000001">>, <<"{\"name\":\"MongoDB\",\"type\":\"database\",\"count\":1,\"info\":{\"x\":203,\"y\":\"102\"}}">>},

    ?assertEqual(Expected, bsondoc_to_json(Input)).

json_test() ->
    Input1 = <<"52f58e20b1297dad7f000001">>,
    Input2 = <<"{\"name\":\"MongoDB\",\"type\":\"database\",\"count\":1,\"info\":{\"x\":203,\"y\":\"102\"}}">>,
    Expected = {'_id',{<<82,245,142,32,177,41,125,173,127,0,0,1>>},<<"name">>,<<"MongoDB">>,<<"type">>,<<"database">>,<<"count">>,1,<<"info">>,{<<"x">>,203,<<"y">>,<<"102">>}},

    ?assertEqual(Expected, json_to_bsondoc(Input1, Input2)).

json_list_test() ->
    Input = <<"[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"type\",\"type\":\"string\"},{\"name\":\"count\",\"type\":\"number\"}]">>,
    Proplist = jsonx:decode(Input, [{format, proplist}]),
    Docs = [rjw_util:proplist_to_doclist(X, []) || X <- Proplist],
    
    ?assertEqual([{<<"name">>,<<"name">>,<<"type">>,<<"string">>},
                  {<<"name">>,<<"type">>,<<"type">>,<<"string">>},
                  {<<"name">>,<<"count">>,<<"type">>,<<"number">>}], Docs).

keys_test() ->
    Input1 = <<"52f58e20b1297dad7f000001">>,
    Input2 = <<"{\"name\":\"MongoDB\",\"type\":\"database\",\"count\":1,\"info\":{\"x\":203,\"y\":\"102\"}}">>,
    Keys = [name, type],
    Expected = {'_id',{<<82,245,142,32,177,41,125,173,127,0,0,1>>},<<"name">>,<<"MongoDB">>,<<"type">>,<<"database">>},

    ?assertEqual(Expected, json_to_bsondoc(Input1, Input2, Keys)).

-endif.