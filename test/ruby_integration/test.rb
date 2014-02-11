require 'rubygems'
require 'mongo'
require 'test/unit'

class DBTest < Test::Unit::TestCase
    include Mongo

    @@client  = Mongo::MongoClient.new("localhost", 27017)
    @@db      = @@client.db("testdb")
    @@coll    = @@db.collection("testCollection1")
    @@version = @@client.server_version

    def test_ping()
        expected = {"ok"=>true}
        assert_equal expected, @@client.ping
    end

    def test_database_info()
        assert_equal ["admin", "riak"], @@client.database_names
        info = { "admin" => 0, "riak" => 0 }
        assert_equal info, @@client.database_info
    end

    def test_insert()
        # nested docs don't work in RJ yet apparently for schema inferral anyway...
        # doc = {"name" => "MongoDB", "type" => "database", "count" => 1, "info" => {"x" => 203, "y" => '102'}}
        doc = {"name" => "MongoDB", "type" => "database", "count" => 1}
        assert @@coll.insert(doc)
    end

    def test_update()
        # doc = {"name" => "MongoDB", "type" => "database", "count" => 1}
        # id = @@coll.insert(doc)

        # doc["name"] = "MongoDB Ruby"
        # assert @@coll.update({"_id" => id}, doc)
        # assert_equal doc, @@coll.find("_id" => id)
        
        # assert @@coll.update({"_id" => id}, {"$set" => {"name" => "MongoDB Riak Ruby"}})
        # doc["name"] = "MongoDB Riak Ruby"
        # assert_equal doc, @@coll.find("_id" => id)
    end

    def test_remove_by()
        # doc = {"name" => "MongoDB", "type" => "database", "count" => 1}
        # id = @@coll.insert(doc)

        # coll.count
        # coll.remove("i" => 71)
        # coll.count
        # puts coll.find("i" => 71).to_a
    end

    def test_remove_all()
        # coll.count
        # coll.remove
        # coll.count
    end

    def test_count()
        # coll.count
    end

    def test_collection_names()
        doc = {"name" => "MongoDB", "type" => "database", "count" => 1}
        assert @@db.collection("testCollection1").insert(doc)
        assert @@db.collection("testCollection2").insert(doc)
        assert @@db.collection("testCollection3").insert(doc)

        names = @@db.collection_names

        assert names.include? "testCollection1"
        assert names.include? "testCollection2"
        assert names.include? "testCollection3"
    end

    def test_find_last_one()
        doc = {"name" => "MongoDB", "type" => "database", "count" => 1}
        id = @@coll.insert(doc)
        one = @@coll.find_one
        assert_equal id, one["_id"]
        assert_equal doc["name"], one["name"]
        assert_equal doc["type"], one["type"]
    end

    def test_find_by_id()
        doc = {"name" => "MongoDB", "type" => "database", "count" => 1}
        id = @@coll.insert(doc)
        one = @@coll.find("_id" => id).to_a.first
        assert_equal id, one["_id"]
        assert_equal doc["name"], one["name"]
        assert_equal doc["type"], one["type"]
    end

    def test_sorted_findall()
        # coll.find.sort(:i)
        # coll.find.sort(:i => :desc)
    end

    def test_findall_by_field()
        # coll.find("i" => 71).to_a # {"_id"=>BSON::ObjectId('4f7b20b4e4d30b35c9000049'), "i"=>71}
    end

    def test_findall_gt_lt()
        # puts coll.find("i" => {"$gt" => 50}).to_a
        # puts coll.find("i" => {"$gt" => 20, "$lte" => 30}).to_a
    end

    def test_find_by_id_selected_fields()
        # puts coll.find({"_id" => id}, :fields => ["name", "type"]).to_a
    end

    def test_findall_regex()
        # puts coll.find({"name" => /^M/}).to_a

        # params = {'search' => 'DB'}
        # search_string = params['search']

        # # Constructor syntax
        # puts coll.find({"name" => Regexp.new(search_string)}).to_a

        # # Literal syntax
        # puts coll.find({"name" => /#{search_string}/}).to_a
    end

    def test_schemas()

        #use db("schema:#{collection}") for schema operations as crud, implement for query (by id) as well

        # # create_index assumes ascending order; see method docs
        # for details
        # coll.create_index("i")

        # # Explicit "ascending"
        # coll.create_index([["i", Mongo::ASCENDING]]) # ruby 1.8
        # coll.create_index(:i => Mongo::ASCENDING) # ruby 1.9 and greater

        # coll.find("_id" => id).explain
        # coll.find("i" => 71).explain
        # coll.find("type" => "database").explain

        # # You can get a list of the indexes on a collection.
        # coll.index_information

        # # ruby 1.9 and greater
        # people.create_index(:loc => Mongo::GEO2D)

        # people.find({"loc" => {"$near" => [50, 50]}}, {:limit => 20}).each do |p|
        #   puts p.inspect
        # end

        # coll.drop_index("i_1")
        # coll.index_information

        # coll.drop_indexes
        # coll.index_information
    end

    def test_collection_drop()
        # coll.drop
        # db.collection_names
        # db.drop_collection("testCollection")
    end

    def test_database_drop()
        # mongo_client.drop_database("mydb")
        # mongo_client.database_names
    end

    def test_miscl_admin()
        # puts db.profiling_level   # => off (the symbol :off printed as a string)
        # db.profiling_level = :slow_only

        # # Validating a collection will return an interesting hash if all is well or raise an exception if there is a problem.
        # p db.validate_collection('coll_name')
    end

    def test_authentication()
        # auth = db.authenticate("myuser", "mypass")
    end
end