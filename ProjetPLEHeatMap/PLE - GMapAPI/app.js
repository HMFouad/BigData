
// Init des var global
var express = require('express');
var app = express();
var path = require('path');
hbase = require("hbase-rpc-client");

//INIT Json var
var jsonData0 = {
    pays:[]
};
var jsonData1 = {
    pays:[]
};
var jsonData2 = {
    pays:[]
};
var jsonData3 = {
    pays:[]
};


// Rendre public l'acces aux donnes statique
app.use(express.static(path.join(__dirname, 'public')));

// Implementation des routes
app.get('/',function(req,res){
    res.sendFile(path.join(__dirname+'/public/index.html'));
});


// Implementation des routes
app.get('/var',function(req,res){
    res.sendFile(path.join(__dirname+'/public/variable.html'));
});

//Def de hBase
client = hbase({
zookeeperHosts: ['localhost'],
zookeeperRoot: '/hbase',
});
client2 = hbase({
zookeeperHosts: ['localhost'],
zookeeperRoot: '/hbase',
});
client3 = hbase({
zookeeperHosts: ['localhost'],
zookeeperRoot: '/hbase',
});


//Recup data zoom 1
const scan1 = client.getScanner('worldCitiesCountry');
var s = scan1.toArray((err, res) =>{
    for(var i in res)
        {
            var pay = {
                "pop":res[i].cols["WorldCitiesByCountry:population"].value.toString('utf8'),
                "lat":res[i].cols["WorldCitiesByCountry:latitude"].value.toString('utf8'),
                "lon":res[i].cols["WorldCitiesByCountry:longitude"].value.toString('utf8')
                
            }
            jsonData1.pays.push(pay);  
        }
    app.get('/wc/pays', function(req,res)
    {
        res.setHeader('Content-Type', 'application/json');
        res.json(jsonData1);
    });
});


// recup data zoom 2
const scan2 = client2.getScanner('worldCitiesRegion');
var s = scan2.toArray((err, res) =>{
    for(var i in res)
        {
            var pay = {
                "pop":res[i].cols["WorldCitiesByRegion:population"].value.toString('utf8'),
                "lat":res[i].cols["WorldCitiesByRegion:latitude"].value.toString('utf8'),
                "lon":res[i].cols["WorldCitiesByRegion:longitude"].value.toString('utf8')
                
            }            
            jsonData2.pays.push(pay);   
        }
    app.get('/wc/region', function(req,res)
    {
        res.setHeader('Content-Type', 'application/json');
        res.json(jsonData2);
    });
});


// recup data zoom 3
const scan3 = client3.getScanner('worldCitiesCity');
var s = scan3.toArray((err, res) =>{
    for(var i in res)
        {
            if(i%2===0)
            {
                    var pay = {
                        "pop":res[i].cols["WorldCitiesByCity:population"].value.toString('utf8'),
                        "lat":res[i].cols["WorldCitiesByCity:latitude"].value.toString('utf8'),
                        "lon":res[i].cols["WorldCitiesByCity:longitude"].value.toString('utf8')
                        
                    }
                    jsonData3.pays.push(pay); 
            } 
        }
    app.get('/wc/ville', function(req,res)
    {
        res.setHeader('Content-Type', 'application/json');
        res.json(jsonData3);
    });
});

//Connex
client.on('error', err => console.log(err));
client2.on('error', err => console.log(err));
client3.on('error', err => console.log(err));

//Lancement serv
app.listen(8001);
