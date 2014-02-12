
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

-module(rjw_message).
-export([
    get_messages/1,
    get_message/1,
    put_message/3,
    put_reply/3,
    get_reply/1
]).
-export_type([db/0]).
-export_type([notice/0, request/0, reply/0]).
-export_type([message/0]).
-export_type([requestid/0]).

-include("rjw_message.hrl").
-include_lib("bson/include/bson_binary.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

% A notice is an asynchronous message sent to the server (no reply expected)
-type notice() :: #insert{} | #update{} | #delete{} | #killcursor{}.
% A request is a syncronous message sent to the server (reply expected)
-type request() :: #'query'{} | #getmore{}.
% A reply to a request
-type reply() :: #reply{}.
 % message id
-type requestid() :: integer().
-type message() :: notice() | request().

% RequestId expected to be in scope at call site
-define(put_header(Opcode), ?put_int32(RequestId), ?put_int32(0), ?put_int32(Opcode)).
-define(put_header(Opcode, ResponseTo), ?put_int32(RequestId), ?put_int32(ResponseTo), ?put_int32(Opcode)).
-define(get_header(Opcode, ResponseTo), ?get_int32(_RequestId), ?get_int32(ResponseTo), ?get_int32(Opcode)).
-define(get_header(Opcode, ResponseTo, RequestId), ?get_int32(RequestId), ?get_int32(ResponseTo), ?get_int32(Opcode)).

-define(ReplyOpcode, 1).
-define(UpdateOpcode, 2001).
-define(InsertOpcode, 2002).
-define(QueryOpcode, 2004).
-define(GetmoreOpcode, 2005).
-define(DeleteOpcode, 2006).
-define(KillcursorOpcode, 2007).

%%% =================================================== server functions

get_messages(MessagesBin) ->
    get_messages(MessagesBin, []).

get_messages(MessagesBin, Messages) ->
    case first_message(MessagesBin) of
        {false, Rest} -> {lists:reverse(Messages), Rest};
        {First, Rest} -> get_messages(Rest, [get_message(First) | Messages])
    end.

get_message(<<?get_header(?QueryOpcode, _, RequestId), R0/binary>>) ->
    <<?get_bits32(0, 0, AD, NCT, 0, SOK, TC, 0), R1/binary>> = R0,
    {Db, Coll, R2} = get_dbcoll(R1),
    <<?get_int32(Skip), ?get_int32(Batch), R3/binary>> = R2,
    {Sel, R4} = bson_binary:get_document(R3),
    {Proj, _} = case R4 of <<>> -> {[], <<>>}; P1 -> bson_binary:get_document(P1) end,

    {Db, #query{
        tailablecursor = bool(TC), slaveok = bool(SOK), nocursortimeout = bool(NCT), awaitdata = bool(AD),
        collection = Coll, skip = Skip, batchsize = Batch, selector = Sel, projector = Proj
    }, RequestId};

get_message(<<?get_header(?InsertOpcode, _, RequestId), ?get_int32(0), R0/binary>>) ->
    {Db, Coll, R1} = get_dbcoll(R0),
    {Docs, _} = get_unknown_docs(R1),

    {Db, #insert{documents = Docs, collection = Coll}, RequestId};

get_message(<<?get_header(?UpdateOpcode, _, RequestId), ?get_int32(0), R0/binary>>) ->
        {Db, Coll, R1} = get_dbcoll(R0),
        <<?get_bits32(0,0,0,0,0,0, M, U), R2/binary>> = R1,
        {Sel, R3} = bson_binary:get_document(R2),
        {Up, _} = bson_binary:get_document(R3),
        
        {Db, #update{collection = Coll, upsert = bool(U), multiupdate = bool(M), selector = Sel, updater = Up}, RequestId};

get_message(<<?get_header(?DeleteOpcode, _, RequestId), ?get_int32(0), R0/binary>>) ->
    {Db, Coll, R1} = get_dbcoll(R0),
    <<?get_bits32(0,0,0,0,0,0,0, R), R2/binary>> = R1,
    {Sel, _} = bson_binary:get_document(R2),

    {Db, #delete{collection = Coll, singleremove = bool(R), selector = Sel}, RequestId};

get_message(<<?get_header(?KillcursorOpcode, _, RequestId), ?get_int32(0), R0/binary>>) ->
    <<?get_int32(CidsLen), R1/binary>> = R0,
    {Cids, _} = get_docs (CidsLen, R1),

    {<<>>, #killcursor{cursorids = Cids}, RequestId};

get_message(<<?get_header(?GetmoreOpcode, _, RequestId), ?get_int32 (0), R0/binary>>) ->
    {Db, Coll, R1} = get_dbcoll(R0),
    <<?get_int32(Batch), ?get_int64(Cid)>> = R1,

    {Db, #getmore{collection = Coll, batchsize = Batch, cursorid = Cid}, RequestId};

get_message(<<?get_header(OpCode, _, RequestId), _/binary>>=Bin) ->
    lager:error("No message type for OpCode: ~p, RequestId: ~p, Full Message: ~p~n", [OpCode, RequestId, Bin]),
    {<<>>, {}, RequestId}.

put_reply(RequestId, ResponseTo, #reply {
        cursornotfound = CursorNotFound,
        queryerror = QueryError,
        awaitcapable = AwaitCapable,
        cursorid = CursorId,
        startingfrom = StartingFrom,
        documents = Docs0
    }) ->
    Docs = case is_list(Docs0) of
        true -> 
            <<?put_int32 (length(Docs0)),
            << <<(bson_binary:put_document(Doc))/binary>> || Doc <- Docs0>>/binary >>;
        false ->
            <<?put_int32 (1), (bson_binary:put_document(Docs0))/binary >>
    end,
    << ?put_header(?ReplyOpcode, ResponseTo),
        ?put_bits32 (0,0,0,0, bit(AwaitCapable), 0, bit(QueryError), bit(CursorNotFound)),
        ?put_int64 (CursorId),
        ?put_int32 (StartingFrom),
        Docs/binary >>.
        


%%% =================================================== client functions


-spec dbcoll (db(), collection()) -> bson:utf8().
%@doc Concat db and collection name with period (.) in between
dbcoll (Db, Coll) -> <<(atom_to_binary (Db, utf8)) /binary, $., (atom_to_binary (Coll, utf8)) /binary>>.

-spec put_message(db(), message(), requestid()) -> binary().
put_message(Db, #insert{collection = Coll, documents = Docs}, RequestId) ->
    <<?put_header(?InsertOpcode),
        ?put_int32(0),
        (bson_binary:put_cstring(dbcoll(Db, Coll))) /binary,
        << <<(bson_binary:put_document(Doc))/binary>> || Doc <- Docs>>/binary >>;
put_message(Db, #update{collection = Coll, upsert = U, multiupdate = M, selector = Sel, updater = Up}, RequestId) ->
    <<?put_header(?UpdateOpcode),
        ?put_int32(0),
        (bson_binary:put_cstring(dbcoll(Db, Coll)))/binary,
        ?put_bits32(0,0,0,0,0,0, bit(M), bit(U)),
        (bson_binary:put_document(Sel))/binary,
        (bson_binary:put_document (Up))/binary>>;
put_message(Db, #delete{collection = Coll, singleremove = R, selector = Sel}, RequestId) ->
    <<?put_header(?DeleteOpcode),
        ?put_int32(0),
        (bson_binary:put_cstring(dbcoll(Db, Coll)))/binary,
        ?put_bits32(0,0,0,0,0,0,0, bit(R)),
        (bson_binary:put_document(Sel))/binary>>;
put_message(_Db, #killcursor{cursorids = Cids}, RequestId) ->
    <<?put_header(?KillcursorOpcode),
        ?put_int32(0),
        ?put_int32(length(Cids)),
        << <<?put_int64(Cid)>> || Cid <- Cids>>/binary >>;
put_message(Db, #'query'{tailablecursor = TC, slaveok = SOK, nocursortimeout = NCT, awaitdata = AD,
        collection = Coll, skip = Skip, batchsize = Batch, selector = Sel, projector = Proj}, RequestId) ->
    <<?put_header(?QueryOpcode),
        ?put_bits32(0, 0, bit(AD), bit(NCT), 0, bit(SOK), bit(TC), 0),
        (bson_binary:put_cstring (dbcoll (Db, Coll)))/binary,
        ?put_int32(Skip),
        ?put_int32(Batch),
        (bson_binary:put_document(Sel))/binary,
        (case Proj of [] -> <<>>; _ -> bson_binary:put_document(Proj) end)/binary >>;
put_message(Db, #getmore{collection = Coll, batchsize = Batch, cursorid = Cid}, RequestId) ->
    <<?put_header(?GetmoreOpcode),
        ?put_int32 (0),
        (bson_binary:put_cstring(dbcoll (Db, Coll))) /binary,
        ?put_int32(Batch),
        ?put_int64(Cid)>>.


-spec get_reply(binary()) -> {requestid(), reply(), binary()}.
get_reply(Message) ->
    <<?get_header(?ReplyOpcode, ResponseTo),
        ?get_bits32 (_,_,_,_, AwaitCapable, _, QueryError, CursorNotFound),
        ?get_int64 (CursorId),
        ?get_int32 (StartingFrom),
        ?get_int32 (NumDocs),
        Bin /binary >> = Message,
    {Docs, BinRest} = get_docs (NumDocs, Bin),
    Reply = #reply {
        cursornotfound = bool(CursorNotFound),
        queryerror = bool(QueryError),
        awaitcapable = bool(AwaitCapable),
        cursorid = CursorId,
        startingfrom = StartingFrom,
        documents = Docs
    },
    {ResponseTo, Reply, BinRest}.

%%% =================================================== internal functions

%% utilities

get_unknown_docs(Bin) ->
    get_unknown_docs(Bin, []).

get_unknown_docs(Bin, Docs) ->
    case bson_binary:get_document(Bin) of
        {Doc, <<>>} -> {lists:reverse([Doc | Docs]), <<>>};
        {Doc, Rest} -> get_unknown_docs(Rest, [Doc | Docs])
    end.

get_docs(0, Bin) -> {[], Bin};
get_docs(NumDocs, Bin) when NumDocs > 0 ->
    {Doc, Bin1} = bson_binary:get_document (Bin),
    {Docs, Bin2} = get_docs (NumDocs - 1, Bin1),
    {[Doc | Docs], Bin2}.

bit(false) -> 0;
bit(true) -> 1.

bool(0) -> false;
bool(1) -> true.

first_message(<<?get_int32(S), R/binary>>) when byte_size(R) >= S-4 ->
    Size = S-4,
    <<First:Size/binary, Rest/binary>> = R,
    {First, Rest};

first_message(R) ->
    {false, R}.

dot(Bin) ->
    {Pos, _} = binary:match (Bin, <<$.>>),
    Pos.

get_dbcoll(Bin) ->
    {DbColl, R} = bson_binary:get_cstring(Bin),
    Dot = dot(DbColl),
    <<Db :Dot /binary, $.:8, Coll /binary>> = DbColl,
    {Db, Coll, R}.

%%% =================================================== tests

-ifdef(TEST).

ismaster_test() ->
    Input = <<58,0,0,0,1,0,0,0,0,0,0,0,212,7,0,0,0,0,0,0,97,100,109,105,110,46,36,99,109,100,0,0,0,0,0,255,255,255,255,19,0,0,0,16,105,115,77,97,115,116,101,114,0,1,0,0,0,0>>,

    {[Message], _} = get_messages(Input),

    {Db, Op, RequestId} = Message,

    ?debugFmt("Message = ~p~n", [Op]),

    ?assertEqual(<<"admin">>, Db),
    ?assertEqual(false, Op#query.tailablecursor),
    ?assertEqual(false, Op#query.slaveok),
    ?assertEqual(false, Op#query.nocursortimeout),
    ?assertEqual(false, Op#query.awaitdata),
    ?assertEqual(<<"$cmd">>, Op#query.collection),
    ?assertEqual(0, Op#query.skip),
    ?assertEqual(-1, Op#query.batchsize),
    ?assertEqual({isMaster,1}, Op#query.selector),
    ?assertEqual([], Op#query.projector),
    ?assertEqual(1, RequestId).

singledoc_test() ->
    BinReply = put_reply(1, 1, #reply {
        documents = {yeah, <<"somethingsomethingsomething">>}
    }),
    {1, RecReply, <<>>} = get_reply(BinReply),
    ?debugFmt("~p~n", [RecReply]),
    ?assertEqual([{yeah, <<"somethingsomethingsomething">>}], RecReply#reply.documents ).

array_test() ->
    BinReply = put_reply(1, 1, #reply {
        documents = [{name,"testdb.testCollection2"},{name,"testdb.testCollection"}]
    }),
    {1, RecReply, <<>>} = get_reply(BinReply),
    ?debugFmt("~p~n", [RecReply]),
    ?assertEqual([{name,"testdb.testCollection2"},
                             {name,"testdb.testCollection"}], RecReply#reply.documents ).
-endif.
