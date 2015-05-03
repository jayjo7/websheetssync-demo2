var express = require('express'),
    MongoClient = require('mongodb').MongoClient,
   // Server = require('mongodb').Server,
    CollectionDriver = require('./collectionDriver').CollectionDriver;
    bodyParser = require('body-parser')

var app = express();
var collectionDriver;
app.set('port', process.env.PORT || 3000);
app.set('mongodb_url', process.MONGO_URL || "mongodb://websheets-demo:websheets-demo123@ds031601.mongolab.com:31601/websheets-demo");
app.set('sheet_uniqueid_column_name', process.SHEET_UNIQUEID_COLUMN_NAME || "UniqueId");

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({extended: true}));


MongoClient.connect(app.get('mongodb_url'), {native_parser:true}, function(err, db) 
  	{
  		if(err)
  		{  console.log(err);
  			console.log("Trouble connecting to the database ...");
  		}
  		else
  		{
  			console.log("Connected to the database sucessfully ...");
  			collectionDriver = new CollectionDriver (db);
  		}

  	});

app.get('/', function(req, res)
	{

		res.send('Get is working');

	});

app.put('/sheetSync', function(req, res)
{
	var result = true;
	var failure=[];

	for(var key in req.body)
	{
		

		console.log( "Working with the worksheet = " + key);

		var data= req.body[key];

		console.log( "Number of records received = " + data.length);


		

		for (i=0; i<data.length; i++)
		{

			for (var keyData in data[i])
			{
				console.log( "data [ " + i  +' ] [' + keyData + " ] = " + data[i][keyData]);
			}

			/** Revist this section of code when working on the ETA.
			if(key ==='orders')
			{
				 collectionDriver.getOneByGivenCriteria(key, app.get('sheet_uniqueid_column_name'), data[i][app.get('sheet_uniqueid_column_name')], function(err, order){
				if(err)
				{

					console.log("Something wrong there is no orders object for given " + data[i][app.get('sheet_uniqueid_column_name')])
					data[i].lastETAUpdateDateTime = new Date();

				}
				else
				{
					var previousETA= order.ETA;
					var previousETAUpdateDateTime = order.lastETAUpdateDateTime;
					console.log('previousETA = ' + previousETA);
					console.log('previousETAUpdateDateTime = ' + previousETAUpdateDateTime);

				}

				});


			}
**/

		   	collectionDriver.upsert(key, data[i], app.get('sheet_uniqueid_column_name') ,function(err,docs) 
		   	{
          		if (err) 
          			{ 
          				console.log(err); 
          				result = false;
          				failure.push(data[i][app.get('sheet_uniqueid_column_name')]);

          			}  
     		});
		}


	}
	if (result)
	{
		var resultObject ={"result": "Success"};

		res.send(resultObject);	
	}
	else
	{
		res.send(failure);
	}

	
});

app.put('/sheetSync/full', function(req,res)
{

	var result = true;
	var failure=[];

	var dataFromDb 

	for(var key in req.body)
	{

		collectionDriver.findAll(key, function(err,results)
		{
			console.log( "Working with the worksheet = " + key);

		    var data= req.body[key];

			if(err)
			{
				console.log("Trouble reteriving the data from mongodb")

			}
			else
			{
				dataFromDb = results;
				console.log("Size of array received from db : " + dataFromDb.length);

		    	console.log( "Number of records received = " + data.length);

		    	for(var i=0; i<data.length; i++)
		    	{
		    		console.log ( "data[i].UniqueId = " + data[i].UniqueId);

		    		for(var keyFromDB in dataFromDb)
		    		{
		    			console.log("Data From DB Key = " + keyFromDB);
		    			console.log("UniqueId = " + dataFromDb[keyFromDB].UniqueId);
		  		    	console.log("_id= " + dataFromDb[keyFromDB]._id);
  		

		    			if(data[i].UniqueId === dataFromDb[keyFromDB].UniqueId)
		    			{
		    				dataFromDb.splice(keyFromDB, 1);
		    				break;
		    			}
		    		}
		    	}
		    }

		    console.log("Size of array received from db after check : " + dataFromDb.length);

		    var fullSyncResult=[];

			for(var keyFromDB in dataFromDb)
		    {	  
		    	collectionDriver.delete(key, dataFromDb[keyFromDB]._id, function(err, results)
		    	{
		    		if(err)
		    		{
		    			console.log ("Trouble deleting the record with _id : " + dataFromDb[keyFromDB]._id);

						failure.push(dataFromDb[keyFromDB]._id)	;    		
					}
		    		else
		    		{
		    			
		    		}
		    	});


		    }	


			for (i=0; i<data.length; i++)
			{

		   			collectionDriver.upsert(key, data[i], app.get('sheet_uniqueid_column_name') ,function(err,docs) 
		   			{
          				if (err) 
          				{ 
          					console.log(err); 
          					result = false;
          					failure.push(data[i][app.get('sheet_uniqueid_column_name')]);

          				}  
     			});
			}


	

		    if(failure.lenght > 0)
		    {
		    	res.send(failure);
		    }
		    else
		    {
		    	var resultObject ={"result": "Full Sync Success"};

		        res.send(resultObject);	
		    }


		});
	
	}
});


app.listen(app.get('port'));
console.log("listening on port : " + app.get('port'));