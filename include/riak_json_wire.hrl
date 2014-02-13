
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

% Wire protocol message types (records)

-type db() :: binary().

-type collection() :: binary(). % without db prefix

-type cursorid() :: integer().

-type selector() :: bson:document().

-record (insert, {
    collection :: collection(),
    documents :: [bson:document()] }).

-record (update, {
    collection :: collection(),
    upsert = false :: boolean(),
    multiupdate = false :: boolean(),
    selector :: selector(),
    updater :: bson:document() | modifier() }).

-type modifier() :: bson:document().

-record (delete, {
    collection :: collection(),
    singleremove = false :: boolean(),
    selector :: selector() }).

-record (killcursor, {
    cursorids :: [cursorid()] }).

-record ('query', {
    tailablecursor = false :: boolean(),
    slaveok = false :: boolean(),
    nocursortimeout = false :: boolean(),
    awaitdata = false :: boolean(),
    collection :: collection(),
    skip = 0 :: skip(),
    batchsize = 0 :: batchsize(),
    selector = {} :: selector(),
    projector = [] :: projector() }).

-type projector() :: bson:document().
-type skip() :: integer().
-type batchsize() :: integer(). % 0 = default batch size. negative closes cursor

-record (getmore, {
    collection :: collection(),
    batchsize = 0 :: batchsize(),
    cursorid :: cursorid() }).

-record (reply, {
    cursornotfound = false :: boolean(),
    queryerror = false :: boolean(),
    awaitcapable = false :: boolean(),
    cursorid = 0 :: cursorid(),
    startingfrom = 0 :: integer(),
    documents = [] :: [bson:document()] }).

% Wire protocol server types (records)

-record (session, {
    last_errors = [],
    last_inserts = []
    }).