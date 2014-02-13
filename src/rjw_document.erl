
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

-module(rjw_document).

-export([
    handle/3
    ]).

-include("riak_json_wire.hrl").

% Field Updaters
-define(inc_up(FieldValues), { '$inc', FieldValues }). %Increments the value of the field by the specified amount.
-define(rename_up(Names), { '$rename', Names}). %Renames a field.
-define(setoninsert_up(FieldValues), { '$setOnInsert', FieldValues }). %Sets the value of a field upon document creation during an upsert. Has no effect on update operations that modify existing documents.
-define(set_up(FieldValues), { '$set', FieldValues }). %Sets the value of a field in an existing document.
-define(unset_up(FieldValues), { '$unset', FieldValues }). %Removes the specified field from an existing document.

% Array Updaters
% -define(increment_updater(FieldValues), { '<some placeholder>.$.<some placeholder>', }). %Acts as a placeholder to update the first element that matches the query condition in an update.
-define(addtoset_arrup(Field, Addition), { '$addToSet', { Field, Addition }}). %Adds elements to an existing array only if they do not already exist in the set.
-define(pop_arrup(Field, FirstLast), { '$pop', { Field, FirstLast }}). %Removes the first (-1) or last (1) item of an array.
-define(pullall_arrup(Field, Values), { '$pullAll', { Field, Values }}). %Removes all matching values from an array.
-define(pull_arrup(Field, Query), { '$pull', { Field, Query }}). %Removes items from an array that match a query statement.
-define(pushall_arrup(Field, Values), { '$pushAll', { Field, Values }}). %Deprecated. Adds several items to an array.
-define(push_arrup(Field, Value), { '$push', { Field, Value }}). %Adds an item to an array.

% Updater Modifiers
-define(each_upmod(Values), { '$each', Values}). %Modifies the $push and $addToSet operators to append multiple items for array updates.
-define(slice_upmod(Num), { '$slice', Num}). %Modifies the $push operator to limit the size of updated arrays.
-define(sort_upmod(SortDoc), { '$sort', SortDoc}). %Modifies the $push operator to reorder documents stored in an array.

% Bitwise Updaters
-define(bit_up(Field, Op, Num), { '$bit', { Field, { Op, Num }}}). %Performs bitwise AND and OR updates of integer values.

% Updater Isolation (doesn't work on sharded clusters, so shouldn't work on Riak?)
% -define(increment_updater(FieldValues), { '$isolated', }). %Modifies behavior of multi-updates to improve the isolation of the operation.

%%% =================================================== external api

handle(_, #insert{collection=_, documents=[]}, Session) -> 
    {noreply, undefined, Session};
handle(Db, #insert{collection=Coll, documents=[Doc|R]}=Command, Session) ->
    %%TODO: Handle Errors
    {Key} = insert_or_update(Db, Coll, Doc),
    NewSession = riak_json_wire:append_last_insert(Db, Coll, Key, Session),
    handle(Db, Command#insert{documents=R}, NewSession);
handle(Db, #update{
        collection = Coll, 
        upsert = U, 
        multiupdate = M, 
        selector = Sel, 
        updater = Updater
        }, Session) ->

    {reply, #reply{documents = Docs}, NewSession} = 
        rjw_query:handle(Db, #query{collection=Coll, selector=Sel}, Session),

    handle_update(Db, Coll, U, M, Sel, Updater, Docs, NewSession);
handle(Db, #delete{collection=Coll,singleremove=Single,selector=Sel}, Session) -> 
    {reply, #reply{documents = Docs}, NewSession} = 
        rjw_query:handle(Db, #query{collection=Coll, selector=Sel}, Session),

    case Docs of
        [{ok, false, err, Reason}] -> {noreply, undefined, Session};
        _ -> handle_delete(Db, Coll, Single, Docs, NewSession)
    end;

handle(Db, Command, Session) -> 
    lager:error("Unhandled Command: ~p on Db: ~p~n", [Command, Db]),
    {noreply, undefined, riak_json_wire:set_last_error(Db, <<"Operation not supported.">>, Session)}.

%%% =================================================== internal functions

insert_or_update(Db, Coll, Doc) ->
    {Key, JDocument} = rjw_util:bsondoc_to_json(Doc),
    riak_json:store_document(<<Db/binary, $.:8, Coll/binary>>, Key, JDocument),
    {Key}.

% Query failed, no upsert
handle_update(Db, _Coll, false, _M, _Sel, _Updater, [], Session) -> 
    error_reply(Db, <<"No results from query, upsert not enabled.">>, Session);
% Query failed, but upsert %TODO: need to handle $setOnInsert: { <field1>: <value1>, ... } here
handle_update(Db, Coll, true, _M, Sel, {First, _}=Updater, [], Session) -> 
    case (string:str(atom_to_list(First), "$") > 0) of
        true -> %using Selector fields
            handle(Db, #insert{collection=Coll, documents=[Sel]}, Session);
        false -> %using Updater fields
            handle(Db, #insert{collection=Coll, documents=[Updater]}, Session)
    end;
% Query succeeded with multiple results, no multi update
handle_update(Db, _Coll, _U, false, _Sel, _Updater, Docs, Session) when is_list(Docs) -> 
    error_reply(Db, <<"Query returned multiple results, but multi update not enabled.">>, Session);
% Query succeeded with multiple results, multi update, or one result and multi doesn't matter
handle_update(Db, Coll, _U, true, _Sel, Updater, Docs, Session) when is_list(Docs) -> 
    perform_update(Db, Coll, Docs, Updater, Session);
handle_update(Db, Coll, _U, _M, _Sel, Updater, Doc, Session) when is_tuple(Doc) -> 
    perform_update(Db, Coll, Doc, Updater, Session);
%% error
handle_update(Db, _, _, _, _, _, _, Session) -> 
    error_reply(Db, <<"Operation not supported.">>, Session).

%% Perform update given an Updater and result set
perform_update(_Db, _Coll, [], _Sel, Session) -> {noreply, undefined, Session};
perform_update(Db, Coll, [Doc|Docs], Sel, Session) -> 
    % Update single doc
    perform_update(Db, Coll, Doc, Sel, Session),
    % Update rest of docs
    perform_update(Db, Coll, Docs, Sel, Session);

% Field Updaters
perform_update(Db, Coll, Doc, ?inc_up(FieldValues), Session) when is_tuple(Doc) ->
    error_reply(Db, <<"Increment operation not supported.">>, Session);
perform_update(Db, Coll, Doc, ?rename_up(Names), Session) when is_tuple(Doc) ->
    error_reply(Db, <<"Rename operation not supported.">>, Session);
perform_update(Db, Coll, Doc, ?setoninsert_up(FieldValues), Session) when is_tuple(Doc) ->
    error_reply(Db, <<"Setoninsert operation not supported.">>, Session);
perform_update(Db, Coll, Doc, ?set_up(FieldValues), Session) when is_tuple(Doc) ->
    NewFields = rjw_util:proplist_replaceall(bson:fields(FieldValues), bson:fields(Doc)),
    NewBsondoc = rjw_util:proplist_to_doclist(NewFields, []),
    {Key} = insert_or_update(Db, Coll, NewBsondoc),
    {noreply, undefined, Session};
perform_update(Db, Coll, Doc, ?unset_up(FieldValues), Session) when is_tuple(Doc) ->
    error_reply(Db, <<"Unset operation not supported.">>, Session);

% Array Updaters
perform_update(Db, Coll, Doc, ?addtoset_arrup(Field, Addition), Session) when is_tuple(Doc) ->
    error_reply(Db, <<"Addtoset operation not supported.">>, Session);
perform_update(Db, Coll, Doc, ?pop_arrup(Field, FirstLast), Session) when is_tuple(Doc) ->
    error_reply(Db, <<"Pop operation not supported.">>, Session);
perform_update(Db, Coll, Doc, ?pullall_arrup(Field, Values), Session) when is_tuple(Doc) ->
    error_reply(Db, <<"Pullall operation not supported.">>, Session);
perform_update(Db, Coll, Doc, ?pull_arrup(Field, Query), Session) when is_tuple(Doc) ->
    error_reply(Db, <<"Pull operation not supported.">>, Session);
perform_update(Db, Coll, Doc, ?pushall_arrup(Field, Values), Session) when is_tuple(Doc) ->
    error_reply(Db, <<"Pushall operation not supported.">>, Session);
perform_update(Db, Coll, Doc, ?push_arrup(Field, Value), Session) when is_tuple(Doc) ->
    error_reply(Db, <<"Push operation not supported.">>, Session);

% Bitwise Updaters
perform_update(Db, Coll, Doc, ?bit_up(Field, Op, Num), Session) when is_tuple(Doc) ->
    error_reply(Db, <<"Bit operation not supported.">>, Session);

% Replace document with Selector contents
perform_update(Db, Coll, Doc, Sel, Session) when is_tuple(Doc) ->
    NewFields = rjw_util:proplist_replaceall(bson:fields(Sel), bson:fields(Doc)),
    NewBsondoc = rjw_util:proplist_to_doclist(NewFields, []),
    insert_or_update(Db, Coll, NewBsondoc),
    {noreply, undefined, Session};

%% error
perform_update(Db, _, _, _, Session) -> 
    error_reply(Db, <<"Operation not supported.">>, Session).

handle_delete(_Db, _Coll, _Single, [], Session) -> {noreply, undefined, Session};
handle_delete(Db, Coll, Single, [Doc|Docs], Session) ->
    handle_delete(Db, Coll, Single, Doc, Session),
    case Single of
        true -> {noreply, undefined, Session};
        false -> handle_delete(Db, Coll, Single, Docs, Session)
    end;
handle_delete(Db, Coll, _Single, Doc, Session) when is_tuple(Doc) ->
    DocList = bson:fields(Doc),
    Key = case proplists:get_value('_id', DocList) of
        undefined -> proplists:get_value(<<"_id">>, DocList);
        {K} -> list_to_binary(rjw_util:bin_to_hexstr(K))
    end,
    riak_json:delete_document(<<Db/binary, $.:8, Coll/binary>>, Key),
    {noreply, undefined, Session}.

error_reply(Db, Reason, Session) ->
    lager:error("Unhandled Document command with Reason: ~p on Db: ~p~n", [Reason, Db]),
    {noreply, undefined, riak_json_wire:set_last_error(Db, Reason, Session)}.