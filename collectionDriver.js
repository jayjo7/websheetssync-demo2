var ObjectID = require('mongodb').ObjectID;

CollectionDriver = function(db) {
  this.db = db;
};

CollectionDriver.prototype.getCollection = function(collectionName, callback) {
  this.db.collection(collectionName, function(error, the_collection) {
    if( error ) callback(error);
    else callback(null, the_collection);
  });
};

//find all objects for a collection
CollectionDriver.prototype.findAll = function(collectionName, callback) {
    this.getCollection(collectionName, function(error, the_collection) { 
      if( error ) callback(error)
      else {
        the_collection.find().toArray(function(error, results) { 
          if( error ) callback(error)
          else callback(null, results)
        });
      }
    });
};


//find a specific object
CollectionDriver.prototype.getOneByGivenCriteria = function(collectionName, searchKey,  serchvalue, callback) 
{ 

        console.log('collectionName = ' + collectionName);
        console.log('searchKey = ' + searchKey);
        console.log('serchvalue = ' + serchvalue);



    this.getCollection(collectionName, function(error, the_collection) {
        if (error) 
          {
            console.log("getOneByGivenCriteria: " + error);
            callback(error);
          }
        else {

          console.log('collectionName = ' + the_collection);

          the_collection.findOne({searchKey:serchvalue},function(error,doc) { 
              if (error) 
              callback(error)
              else callback(null, doc);
            });

        }
    });

}


//find a specific object
CollectionDriver.prototype.get = function(collectionName, id, callback) 
{ 
    this.getCollection(collectionName, function(error, the_collection) 
    {
        if (error) callback(error)
        else 
        {
            var checkForHexRegExp = new RegExp("^[0-9a-fA-F]{24}$"); 
            if (!checkForHexRegExp.test(id)) callback({error: "invalid id"});
            else the_collection.findOne({'_id':ObjectID(id)}, function(error,doc) { 
            	if (error) callback(error)
            	else callback(null, doc);
            });
        }
    });
}

//save new object
CollectionDriver.prototype.save = function(collectionName, obj, callback) {
    this.getCollection(collectionName, function(error, the_collection) { 
      if( error ) callback(error)
      else {
        obj.created_at = new Date(); 
        the_collection.insert(obj, function() { 
          callback(null, obj);
        });
      }
    });
};

//update a specific object
CollectionDriver.prototype.update = function(collectionName, obj, entityId, callback) {
    this.getCollection(collectionName, function(error, the_collection) {
        if (error) callback(error)
        else {
	        obj._id = ObjectID(entityId); 
	        obj.updated_at = new Date(); 
            the_collection.save(obj, function(error,doc) { 
            	if (error) callback(error)
            	else callback(null, obj);
            });
        }
    });
}


//update a specific object
CollectionDriver.prototype.upsert = function(collectionName, obj, uniqueIdName, callback) 
{
    this.getCollection(collectionName, function(error, the_collection) 
    {
        if (error) 
        {	
        	callback(error)
        }
        else 
        {

        	console.log("obj[" + uniqueIdName +"] = " + obj[uniqueIdName]);

        	var queryObject = {};
        	queryObject[uniqueIdName]= obj[uniqueIdName];


        	the_collection.findAndModify(
        							queryObject,
        							{},
        							obj,
        							{new:true, upsert:true},
        							function(error,doc) 
        								{ 
            								if (error) 
            								{
            									console.log("Error  in upsert")
            									callback(error)
            								}
            								else 
            								{
            									//console.log("Upsert Sucessfull, here is the doc:" + doc);
            									callback(null, obj);
            								}
            							}	


        	);




/**        	the_collection.update(
        							{uniqueIdName: obj[uniqueIdName]},
        							obj,
        							{upsert:true},
        							function(error,doc) 
        								{ 
            								if (error) 
            								{
            									console.log("Error  in upsert")
            									callback(error)
            								}
            								else 
            								{
            									console.log("Upsert Sucessfull, here is the doc:" + obj);
            									callback(null, obj);
            								}
            							});
**/

        }
    });
}



//delete a specific object
CollectionDriver.prototype.delete = function(collectionName, entityId, callback) {
    this.getCollection(collectionName, function(error, the_collection) { 
        if (error) callback(error)
        else {
            the_collection.remove({'_id':ObjectID(entityId)}, function(error,doc) { 
            	if (error) callback(error)
            	else callback(null, doc);
            });
        }
    });
}

exports.CollectionDriver = CollectionDriver;