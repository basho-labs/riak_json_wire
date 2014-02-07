
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

-module(rjw_command_admin).

-export([
    handle/1
    ]).

-include("rjw_message.hrl").

%%% =================================================== external api

%% Aggregation Commands

% aggregate   Performs aggregation tasks such as group using the aggregation framework.
% count   Counts the number of documents in a collection.
% distinct    Displays the distinct values found for a specified key in a collection.
% group   Groups documents in a collection by the specified key and performs simple aggregation.
% mapReduce   Performs map-reduce aggregation for large data sets.

%% Geospatial Commands

% geoNear Performs a geospatial query that returns the documents closest to a given point.
% geoSearch   Performs a geospatial query that uses MongoDB’s haystack index functionality.
% geoWalk An internal command to support geospatial queries.

%% Query and Write Operation Commands

% findAndModify   Returns and modifies a single document.
% text    Performs a text search.
% getLastError    Returns the success status of the last operation.
% getPrevError    Returns status document containing all errors since the last resetError command.
% resetError  Resets the last error status.
% eval    Runs a JavaScript function on the database server.

%% Database Operations

%% Authentication Commands

% logout  Terminates the current authenticated session.
% authenticate    Starts an authenticated session using a username and password.
% copydbgetnonce  This is an internal command to generate a one-time password for use with the copydb command.
% getnonce    This is an internal command to generate a one-time password for authentication.

%% Replication Commands

% replSetFreeze   Prevents the current member from seeking election as primary for a period of time.
% replSetGetStatus    Returns a document that reports on the status of the replica set.
% replSetInitiate Initializes a new replica set.
% replSetMaintenance  Enables or disables a maintenance mode, which puts a secondary node in a RECOVERING state.
% replSetReconfig Applies a new configuration to an existing replica set.
% replSetStepDown Forces the current primary to step down and become a secondary, forcing an election.
% replSetSyncFrom Explicitly override the default logic for selecting a member to replicate from.
% resync  Forces a mongod to re-synchronize from the master. For master-slave replication only.
% applyOps    Internal command that applies oplog entries to the current data set.

% isMaster    Displays information about this member’s role in the replica set, including whether it is the master.
handle(#query{collection= <<"$cmd">>, selector= {ismaster, 1}}=Collection) ->
    handle(Collection#query{selector={isMaster, 1}});
handle(#query{collection= <<"$cmd">>, selector= {isMaster, 1}}) -> 
    Docs = [{ok, true, ismaster, 1}],
    #reply{documents = Docs};

% getoptime   Internal command to support replication, returns the optime.

% %% Sharding Commands

% flushRouterConfig   Forces an update to the cluster metadata cached by a mongos.
% addShard    Adds a shard to a sharded cluster.
% checkShardingIndex  Internal command that validates index on shard key.
% enableSharding  Enables sharding on a specific database.
% listShards  Returns a list of configured shards.
% removeShard Starts the process of removing a shard from a sharded cluster.
% getShardMap Internal command that reports on the state of a sharded cluster.
% getShardVersion Internal command that returns the config server version.
% setShardVersion Internal command to sets the config server version.
% shardCollection Enables the sharding functionality for a collection, allowing the collection to be sharded.
% shardingState   Reports whether the mongod is a member of a sharded cluster.
% unsetSharding   Internal command that affects connections between instances in a MongoDB deployment.
% split   Creates a new chunk.
% splitChunk  Internal command to split chunk. Instead use the methods sh.splitFind() and sh.splitAt().
% splitVector Internal command that determines split points.
% medianKey   Deprecated internal command. See splitVector.
% moveChunk   Internal command that migrates chunks between shards.
% movePrimary Reassigns the primary shard when removing a shard from a sharded cluster.
% isdbgrid    Verifies that a process is a mongos.

%% Instance Administration Commands

% renameCollection    Changes the name of an existing collection.
% copydb  Copies a database from a remote host to the current host.
% dropDatabase    Removes the current database.
% drop    Removes the specified collection from the database.
% create  Creates a collection and sets collection parameters.
% clone   Copies a database from a remote host to the current host.
% cloneCollection Copies a collection from a remote host to the current host.
% cloneCollectionAsCapped Copies a non-capped collection as a new capped collection.
% closeAllDatabases   Internal command that invalidates all cursors and closes open database files.
% convertToCapped Converts a non-capped collection to a capped collection.
% filemd5 Returns the md5 hash for files stored using GridFS.
% dropIndexes Removes indexes from a collection.
% fsync   Flushes pending writes to the storage layer and locks the database to allow backups.
% clean   Internal namespace administration command.
% connPoolSync    Internal command to flush connection pool.
% compact Defragments a collection and rebuilds the indexes.
% collMod Add flags to collection to modify the behavior of MongoDB.
% reIndex Rebuilds all indexes on a collection.
% setParameter    Modifies configuration options.
% getParameter    Retrieves configuration options.
% repairDatabase  Repairs any errors and inconsistencies with the data storage.
% touch   Loads documents and indexes from data storage to memory.
% shutdown    Shuts down the mongod or mongos process.
% logRotate   Rotates the MongoDB logs to prevent a single file from taking too much space.

%% Diagnostic Commands

% listDatabases   Returns a document that lists all databases and returns basic database statistics.
handle(#query{collection= <<"$cmd">>, selector= {listDatabases,1}}) ->
    Docs = [{ok, true, databases, [{name, <<"admin">>, sizeOnDisk, 0, empty, true},{name, <<"riak">>, sizeOnDisk, 0, empty, false}]}],
    #reply{documents = Docs};

% dbHash  Internal command to support sharding.
% driverOIDTest   Internal command that converts an ObjectID to a string to support tests.
% listCommands    Lists all database commands provided by the current mongod instance.
% availableQueryOptions   Internal command that reports on the capabilities of the current MongoDB instance.
% buildInfo   Displays statistics about the MongoDB build.
% collStats   Reports storage utilization statics for a specified collection.
% connPoolStats   Reports statistics on the outgoing connections from this MongoDB instance to other MongoDB instances in the deployment.
% dbStats Reports storage utilization statistics for the specified database.
% cursorInfo  Reports statistics on active cursors.
% dataSize    Returns the data size for a range of data. For internal use.
% diagLogging Provides a diagnostic logging. For internal use.
% getCmdLineOpts  Returns a document with the run-time arguments to the MongoDB instance and their parsed options.
% netstat Internal command that reports on intra-deployment connectivity. Only available for mongos instances.
% ping    Internal command that tests intra-deployment connectivity.
% profile Interface for the database profiler.
% validate    Internal command that scans for a collection’s data and indexes for correctness.
% top Returns raw usage statistics for each database in the mongod instance.
% indexStats  Experimental command that collects and aggregates statistics on all indexes.
% whatsmyuri  Internal command that returns information on the current client.
% getLog  Returns recent log messages.
% hostInfo    Returns data that reflects the underlying host system.
% serverStatus    Returns a collection metrics on instance-wide resource utilization and status.
% features    Reports on features available in the current MongoDB instance.
% isSelf  Internal command to support testing.

%% Internal Commands

% handshake   Internal command.
% _recvChunkAbort Internal command that supports chunk migrations in sharded clusters. Do not call directly.
% _recvChunkCommit    Internal command that supports chunk migrations in sharded clusters. Do not call directly.
% _recvChunkStart Internal command that facilitates chunk migrations in sharded clusters.. Do not call directly.
% _recvChunkStatus    Internal command that returns data to support chunk migrations in sharded clusters. Do not call directly.
% _replSetFresh   Internal command that supports replica set election operations.
% mapreduce.shardedfinish Internal command that supports map-reduce in sharded cluster environments.
% _transferMods   Internal command that supports chunk migrations. Do not call directly.
% replSetHeartbeat    Internal command that supports replica set operations.
% replSetGetRBID  Internal command that supports replica set operations.
% _migrateClone   Internal command that supports chunk migration. Do not call directly.
% replSetElect    Internal command that supports replica set functionality.
% writeBacksQueued    Internal command that supports chunk migrations in sharded clusters.
% writebacklisten Internal command that supports chunk migrations in sharded clusters.

%% Testing Commands

% testDistLockWithSkew    Internal command. Do not call this directly.
% testDistLockWithSyncCluster Internal command. Do not call this directly.
% captrunc    Internal command. Truncates capped collections.
% emptycapped Internal command. Removes all documents from a capped collection.
% godinsert   Internal command for testing.
% _hashBSONElement    Internal command. Computes the MD5 hash of a BSON element.
% _journalLatencyTest Tests the time required to write and perform a file system sync for a file in the journal directory.
% sleep   Internal command for testing. Forces MongoDB to block all operations.
% replSetTest Internal command for testing replica set functionality.
% forceerror  Internal command for testing. Forces a user assertion exception.
% skewClockCommand    Internal command. Do not call this command directly.
% configureFailPoint  Internal command for testing. Configures failure points.

%% Undefined Command
handle(_) -> {error, undefined}.
