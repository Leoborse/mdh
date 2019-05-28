/*******************************************************************************
Accessi al database
*******************************************************************************/
const Mongo = require('mongodb');

var MongoDBHelper = function(cfg) {
  const cdb = cfg.database;
  Mongo.MongoClient.connect(cdb.url, {useNewUrlParser:true}, function(err, client) {
    if ( err ) {
      cfg.log({
        status: "Error",
        name: "MongoDBHelper",
        value: {
          url: cdb.url,
          message: err
        }
      });
    } else {
      cfg.log({
        status: "Info",
        name: "MongoDBHelper",
        value: {
          dbserver: client.s
        }
      });
      var dbinfo = client.s.options;
      var dbName = dbinfo.dbName;
      var db = client.db(dbName);
      var bucket = new Mongo.GridFSBucket(db, cdb.grid);
      cdb.dbconfig = {
        oid: Mongo.ObjectId,
        coll: {},
        bucket: bucket,
        tmp: {},
        db: db,
        collcreatersp: function(err, res) {
          if (err) throw err;
          cfg.log({
            status: "Info",
            name: "MongoDBHelper",
            value: {
              entity: "Collezione",
              status: "OK",
              message: res.s.name + "creata !"
            }
          });
          var collName = res.s.name;
          var index = dbc.tmp[collName];
          dbc.coll[index] = db.collection(collName);
          var collIndx = cdb.collections[index].indexes;
          creaIndici(cfg, dbc.coll[index], collIndx);
          if ( --cdb.dbconfig.missingCollections == 0 ) {
            checkInitialContent(cdb);
          }
        },
        missingCollections: 0
      };
      var dbc = cdb.dbconfig;
      db.listCollections().toArray(function(err,rsp){
        var colls = {};
        for ( var i=0; i<rsp.length; i++ ) {
          colls[rsp[i].name] = "Trovata";
        }
        for(var index in cdb.collections) {
          if (cdb.collections.hasOwnProperty(index)) {
            var collName = cdb.collections[index].name;
            dbc.tmp[collName] = index;

            if ( typeof colls[collName] == 'undefined' ) {
              cfg.log({
                status: "Info",
                name: "MongoDBHelper",
                value: {
                  entity: "Collezione",
                  status: "KO",
                  name: collName,
                  message: "Non trovata"
                }
              });
              cdb.dbconfig.missingCollections++;
              db.createCollection(collName, cdb.dbconfig.collcreatersp);
            } else {
              cfg.log({
                status: "Info",
                name: "MongoDBHelper",
                value: {
                  entity: "Collezione",
                  status: "OK",
                  name: collName,
                  message: "Trovata"
                }
              });
              dbc.coll[index] = db.collection(collName)
              var collIndx = cdb.collections[index].indexes;
              creaIndici(cfg, dbc.coll[index], collIndx);
            }
          }
        }
        if ( cdb.dbconfig.missingCollections == 0 ) {
          checkInitialContent(cdb);
        } else {
          cfg.log({
            status: "Warning",
            name: "MongoDBHelper",
            value: {
              entity: "Collezione",
              status: "KO",
              message: cdb.dbconfig.missingCollections + " collezioni mancanti"
            }
          });
        }
      });
    }
  });
}

const checkInitialContent = function(cdb) {
  delete cdb.dbconfig.tmp;
  delete cdb.dbconfig.db;
  delete cdb.dbconfig.collcreatersp;
  delete cdb.dbconfig.missingCollections;
};

const creaIndici = function(cfg, coll, collIndx){
  // Creazione indici
  if ( typeof collIndx != 'undefined' ) {
    for ( var i=0; i<collIndx.length; i++ ) {
      coll.createIndex(
        collIndx[i].keys,
        collIndx[i].options,
        function(err, result) {
          if ( err ) {
            cfg.log({
              status: "Warning",
              name: "MongoDBHelper",
              value: {
                entity: "Indice",
                status: err
              }
            });
          } else {
            cfg.log({
              status: "Info",
              name: "MongoDBHelper",
              value: {
                entity: "Indice",
                status: "OK",
                name: result
              }
            });
          }
        }
      );
    }
  }
}

MongoDBHelper.prototype.get = function(obj) {
  return replaceInKeys(obj,/^__/,'$');
}

MongoDBHelper.prototype.set = function(obj) {
  return replaceInKeys(obj,/^\$/,'__');
}

/*******************************************************************************
Rimpiazza i nomi delle proprietà di un JSON sostituendo replacer al posto della 
regexp
*******************************************************************************/
const replaceInKeys = function(obj, regexp, replacer) {
  obj = Object.assign({}, obj);

  for (let [key, val] of Object.entries(obj)) {
    if (regexp.test(key)) {
      let oldKey = key;
      key = oldKey.replace(regexp, replacer);
      obj[key] = obj[oldKey];
      delete obj[oldKey];
    }
    // If value is an object, recursively go through its keys.
    if ( typeof val === 'object' && key != '_id') {
      // Watch out for arrays. Iterate over it's values and make replacements
      // only in objects, leaving as is strings, numbers and other types.
      if (Array.isArray(val)) {
        val = val.map(function(item) {
          if (typeof item === 'object') return replaceInKeys(item, regexp, replacer);
          else return item;
        });
        obj[key] = val;
      } else {
        obj[key] = replaceInKeys(val, regexp, replacer);
      }
    }
  }
  return obj;
}

module.exports = MongoDBHelper;