require 'rubygems'
require 'mongo'

puts "-------------- Connecting"
mongo_client = Mongo::MongoClient.new("localhost", 27017)

# p mongo_client.database_names
# mongo_client.database_info.each { |info| puts info.inspect }

db = mongo_client.db("testdb")

# auth = db.authenticate("myuser", "mypass")

coll = db.collection("testCollection1")

# nested docs don't work in RJ yet apparently for schema inferral anyway...
# doc = {"name" => "MongoDB", "type" => "database", "count" => 1, "info" => {"x" => 203, "y" => '102'}}
doc = {"name" => "MongoDB", "type" => "database", "count" => 1}

id = coll.insert(doc)
p id

# db.collection("testCollection2").insert(doc)
# db.collection("testCollection3").insert(doc)

# mongo_client.db("testdb1").collection("testCollection1").insert(doc)

p db.collection_names # ["testCollection", "system.indexes"]
p mongo_client.db("testdb1").collection_names

p coll.find_one # { "_id" => BSON::ObjectId('4f7b1ea6e4d30b35c9000001'), "name"=>"MongoDB", "type"=>"database", "count"=>1, "info"=>{"x"=>203, "y"=>"102"}}
p coll.find("_id" => id).to_a

# coll.find.each { |row| puts row.inspect }

# puts coll.find.to_a

# coll.find("i" => 71).to_a # {"_id"=>BSON::ObjectId('4f7b20b4e4d30b35c9000049'), "i"=>71}

# coll.find.sort(:i)

# coll.find.sort(:i => :desc)

# coll.count

# puts coll.find("i" => {"$gt" => 50}).to_a

# puts coll.find("i" => {"$gt" => 20, "$lte" => 30}).to_a

# puts coll.find({"_id" => id}, :fields => ["name", "type"]).to_a

# puts coll.find({"name" => /^M/}).to_a

# params = {'search' => 'DB'}
# search_string = params['search']

# # Constructor syntax
# puts coll.find({"name" => Regexp.new(search_string)}).to_a

# # Literal syntax
# puts coll.find({"name" => /#{search_string}/}).to_a

# doc["name"] = "MongoDB Ruby"
# coll.update({"_id" => id}, doc)

# coll.update({"_id" => id}, {"$set" => {"name" => "MongoDB Ruby"}})

# puts coll.find("_id" => id).to_a

# coll.count
# coll.remove("i" => 71)
# coll.count
# puts coll.find("i" => 71).to_a

# coll.remove
# coll.count

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

# coll.drop
# db.collection_names

# db.drop_collection("testCollection")

# mongo_client.drop_database("mydb")
# mongo_client.database_names

# puts db.profiling_level   # => off (the symbol :off printed as a string)
# db.profiling_level = :slow_only

# # Validating a collection will return an interesting hash if all is well or raise an exception if there is a problem.
# p db.validate_collection('coll_name')
