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
        doc = {"name" => "MongoDB", "type" => "database", "count" => 1}
        id = @@coll.insert(doc)

        doc["name"] = "MongoDB Ruby"
        assert @@coll.update({"_id" => id}, doc)
        assert_equal "MongoDB Ruby", @@coll.find("_id" => id).to_a.first["name"]
        
        assert @@coll.update({"_id" => id}, {"$set" => {"name" => "MongoDB Riak Ruby"}})
        doc["name"] = "MongoDB Riak Ruby"
        assert_equal "MongoDB Riak Ruby", @@coll.find("_id" => id).to_a.first["name"]
    end

    def test_remove_by_id()
        doc = {"name" => "MongoDB", "type" => "database", "count" => 1}
        id = @@coll.insert(doc)

        @@coll.remove("_id" => id)
        assert_equal [], @@coll.find("_id" => id).to_a
    end

    def test_remove_by_field()
        doc = {"name" => "MongoDB", "type" => "database", "count" => 1}
        id = @@coll.insert(doc)

        @@coll.remove("count" => 1)
        assert_equal [], @@coll.find("count" => 1).to_a
    end

    def test_remove_all()
        doc = {"name" => "MongoDB", "type" => "database", "count" => 1}

        @@coll.insert(doc)
        @@coll.insert(doc)
        @@coll.insert(doc)
        @@coll.insert(doc)
        @@coll.insert(doc)

        count = @@coll.count

        # Sometimes there is a ~1 second delay for docs to get indexed
        if count < 5
            sleep(2)
            count = @@coll.count
        end
        
        @@coll.remove
        
        assert_equal 0, @@coll.count
    end

    def test_count()
        doc = {"name" => "MongoDB", "type" => "database", "count" => 1}
        
        @@coll.insert(doc)
        @@coll.insert(doc)
        @@coll.insert(doc)
        @@coll.insert(doc)
        @@coll.insert(doc)

        count = @@coll.count

        # Sometimes there is a ~1 second delay for docs to get indexed
        if count < 5
            sleep(2)
            count = @@coll.count
        end

        assert count >= 5
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

    def test_findall()
        doc = {"name" => "MongoDB", "type" => "database", "count" => 1}
        
        @@coll.insert(doc)
        @@coll.insert(doc)
        @@coll.insert(doc)
        @@coll.insert(doc)
        @@coll.insert(doc)

        results = @@coll.find.to_a

        # Sometimes there is a ~1 second delay for docs to get indexed
        if results.length < 5
            sleep(2)
            results = @@coll.find.to_a
        end

        assert results.length >= 5
    end

    def test_sorted_findall()
        # coll.find.sort(:i)
        # coll.find.sort(:i => :desc)
    end

    def test_findall_by_field()
        coll = @@db.collection("testCollection4")
        coll.remove
        coll.insert({"i" => 10})
        coll.insert({"i" => 20})
        coll.insert({"i" => 30})
        coll.insert({"i" => 40})
        coll.insert({"i" => 50})
        results = coll.find("i" => 30).to_a

        # Sometimes there is a ~1 second delay for docs to get indexed
        if results.nil? || results == []
            sleep(2)
            results = @@db.collection("testCollection4").find("i" => 30).to_a
        end

        assert_equal 30, results.first["i"]
    end

    def test_findall_gt_lt()
        coll = @@db.collection("testCollection4")
        coll.remove
        coll.insert({"i" => 10})
        coll.insert({"i" => 20})
        coll.insert({"i" => 30})
        coll.insert({"i" => 40})
        coll.insert({"i" => 50})
        
        result1 = coll.find("i" => {"$gt" => 20, "$lte" => 40}).to_a

        if result1.nil? || result1.length < 2
            sleep(2)
            result1 = coll.find("i" => {"$gt" => 20, "$lte" => 40}).to_a
        end

        result2 = coll.find("i" => {"$gt" => 50}).to_a

        assert result1.length == 2
        assert result2.length == 0
    end

    def test_find_by_id_selected_fields()
        doc = {"name" => "MongoDB", "type" => "database", "count" => 1}
        id = @@coll.insert(doc)
        one = @@coll.find({"_id" => id}, :fields => ["name", "type"]).to_a.first

        assert_equal "MongoDB", one["name"]
        assert_equal "database", one["type"]
        assert_equal nil, one["count"]
        assert_equal id, one["_id"]

    end

    def test_findall_regex()
        coll = @@db.collection("testCollection5")
        coll.remove
        coll.insert({"name" => "Risk1"})
        coll.insert({"name" => "Riak2"})
        coll.insert({"name" => "Riak3"})
        coll.insert({"name" => "Riak3"})
        coll.insert({"name" => "Mongo5"})

        sleep(2)

        assert_equal 4, coll.find({"name" => /^R/}).to_a.length
        assert_equal 2, coll.find({"name" => /3$/}).to_a.length

        params = {'search' => 'ia'}
        search_string = params['search']

        # Constructor syntax
        assert_equal 3, coll.find({"name" => Regexp.new(search_string)}).to_a.length

        # # Literal syntax
        assert_equal 3, coll.find({"name" => /#{search_string}/}).to_a.length
    end

    def test_schema_inferral()
        doc = {"name" => "MongoDB", "type" => "database", "count" => 1}
        id = @@coll.insert(doc)

        schema = @@client.db("schema:testdb").collection("testCollection1").find_one

        expected_schema = {"fields"=>[
            {"name"=>"name", "type"=>"string"},
            {"name"=>"type", "type"=>"string"},
            {"name"=>"count", "type"=>"number"}
        ]}

        assert_equal expected_schema, schema
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