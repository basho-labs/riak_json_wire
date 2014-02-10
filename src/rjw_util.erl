
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

-module(rjw_util).

-export([
    % bsonid_to_binary/1,
    bsondoc_to_json/1,
    json_to_bsondoc/2
    ]).

-include_lib("bson/include/bson_binary.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

% bsonid_to_binary(<<Size:32/little-unsigned, 16#07, Id:Size/binary, _/binary>>) ->
%     iolist_to_binary("OID:" ++ hexencode(Id));
% bsonid_to_binary(<<Size:32/little-unsigned, 16#00, Id:Size/binary, _/binary>>) ->
%     iolist_to_binary("BIN:" ++ hexencode(Id));
% bsonid_to_binary(<<Size:32/little-unsigned, 16#03, Id:Size/binary, _/binary>>) ->
%     iolist_to_binary("UUID:" ++ hexencode(Id));
% bsonid_to_binary(<<Size:32/little-unsigned, 16#04, Id:Size/binary, _/binary>>) ->
%     iolist_to_binary("MD5:" ++ hexencode(Id));
% bsonid_to_binary(Id) when is_binary(Id) -> ?debugFmt("um", []), hexencode(Id).
% bsonid_to_binary(null) ->
%     iolist_to_binary("UUID:" ++
%         hexencode(list_to_binary(riak_core_util:unique_id_62()))).

json_to_bsondoc(Key, Json) ->
    Proplist = jsonx:decode(Json, [{format, proplist}]),
    WithId = [{<<"_id">>, {Key}} | Proplist],

    DocList = json_to_doclist(WithId, []).

json_to_doclist([], Doclist) ->
    bson:document(lists:reverse(Doclist));
json_to_doclist([{K, Doc} | R], Doclist) when is_tuple(Doc) ->
    json_to_doclist(R, [{K, Doc} | Doclist]);
json_to_doclist([{K, Doc} | R], Doclist) when is_list(Doc) ->
    json_to_doclist(R, [{K, json_to_doclist(Doc, [])} | Doclist]);
json_to_doclist([{K, Doc} | R], Doclist)->
    json_to_doclist(R, [{K, Doc} | Doclist]).

bsondoc_to_json(Doc) ->
    DocList = bson:fields(Doc),
    {Key} = proplists:get_value('_id', DocList),
    WithoutId = proplists:delete('_id', DocList),
    {Key, jsonx:encode(doclist_to_json(WithoutId, []))}.

doclist_to_json([], Doclist) ->
    lists:reverse(Doclist);
doclist_to_json([{K, Doc} | R], Doclist) when is_tuple(Doc) ->
    doclist_to_json(R, [{K, doclist_to_json(bson:fields(Doc), [])} | Doclist]);
doclist_to_json([{K, Doc} | R], Doclist) when is_list(Doc) ->
    doclist_to_json(R, [{K, doclist_to_json(Doc, [])} | Doclist]);
doclist_to_json([{K, Doc} | R], Doclist)->
    doclist_to_json(R, [{K, Doc} | Doclist]).

% hexencode(<<>>) -> [];
% hexencode(<<CH, Rest/binary>>) ->
%     [ hex(CH) | hexencode(Rest) ].

% hex(CH) when CH < 16 ->
%     [ $0, integer_to_list(CH, 16) ];
% hex(CH) ->
%     integer_to_list(CH, 16).


-ifdef(TEST).

bsondoc_test() ->
    Input = {'_id',{<<82,245,142,32,177,41,125,173,127,0,0,1>>},name,<<"MongoDB">>,type,<<"database">>,count,1,info,{x,203,y,<<"102">>}},
    Expected = {<<82,245,142,32,177,41,125,173,127,0,0,1>>, <<"{\"name\":\"MongoDB\",\"type\":\"database\",\"count\":1,\"info\":{\"x\":203,\"y\":\"102\"}}">>},

    ?assertEqual(Expected, bsondoc_to_json(Input)).

json_test() ->
    Input1 = <<82,245,142,32,177,41,125,173,127,0,0,1>>,
    Input2 = <<"{\"name\":\"MongoDB\",\"type\":\"database\",\"count\":1,\"info\":{\"x\":203,\"y\":\"102\"}}">>,
    Expected = {<<"_id">>,{<<82,245,142,32,177,41,125,173,127,0,0,1>>},<<"name">>,<<"MongoDB">>,<<"type">>,<<"database">>,<<"count">>,1,<<"info">>,{<<"x">>,203,<<"y">>,<<"102">>}},

    ?assertEqual(Expected, json_to_bsondoc(Input1, Input2)).
-endif.