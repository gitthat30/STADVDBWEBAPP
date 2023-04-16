
// import module `express`
const express = require('express');
var mysql = require('mysql2');

var connection, connection2, connection3

const connect0 = () => {
    connection = mysql.createConnection({
        host     : 'ccscloud3.dlsu.edu.ph',
        port     : 39066,
        user     : 'root',
        password : 'Asdzxc123!',
        database : 'imdb_full'
    });
    
    connection.connect((error) => {
        if(error) {
            console.log(error)
            node0 = false;
        }
        else {
            node0 = true;
            console.log("Successful Connection to CCSCLOUD0 - imdb_full")
        }
    })

}

const connect1 = () => {
    connection2 = mysql.createConnection({
        host     : 'ccscloud3.dlsu.edu.ph',
        port     : 39067,
        user     : 'root',
        password : 'Asdzxc123!',
        database : 'imdb_b1980'
    });
    
    connection2.connect((error) => {
        if(error) {
            console.log(error)
            node1 = false;
        }
        else {
            node1 = true;
            console.log("Successful Connection to CCSCLOUD1 - imdb_b1980")
        }
    })
}

const connect2 = () => {
    connection3 = mysql.createConnection({
        host     : 'ccscloud3.dlsu.edu.ph',
        port     : 39068,
        user     : 'root',
        password : 'Asdzxc123!',
        database : 'imdb_a1980'
    });
    
    connection3.connect((error) => {
        if(error) {
            console.log(error)
            node2 = false;
        }
        else {
            node2 = true;
            console.log("Successful Connection to CCSCLOUD2 - imdb_a1980")
        }
    })
}

connect0()
connect1()
connect2()

let node0, node1, node2

// import module `hbs`
const hbs = require('hbs');
hbs.registerHelper('Equal', function(x, y) {
    return (x == y) ? true : false;
  });

const app = express();
app.use(express.static('script')); 
const port = 9090;

// set `hbs` as view engine
app.set('view engine', 'hbs');

// parses incoming requests with urlencoded payloads
app.use(express.urlencoded({extended: true}));

// binds the server to a specific port
app.listen(port, function () {
    console.log('app listening at port ' + port);
});

const testConnections = () => {
    flag0 = (connection._protocolError == null) && (node0 == true) 
    flag1 = (connection2._protocolError == null) && (node1 == true)
    flag2 = (connection3._protocolError == null) && (node2 == true)
}

const attemptReconnect = () => {
    if (flag0 == false) {
        //Atttempt reconnect
        console.log("Attempting to reconnect to CCS CLOUD0")
        connect0();
    }
    if (flag1 == false) {
        //Atttempt reconnect
        console.log("Attempting to reconnect to CCS CLOUD1")
        connect1();
    }
    if (flag2 == false) {
        //Atttempt reconnect
        console.log("Attempting to reconnect to CCS CLOUD2")
        connect2();
    }
}

// Routes

app.get('/', function(req, res) {
    

    testConnections()
    attemptReconnect()
    
    res.render('first')
})

app.get('/querysql', async function(req, res) {

    result = await new Promise ((resolve) => {
        connection.query('SELECT * FROM movies WHERE id = ' + req.query.id, function (error, results, fields) {
            if (error) throw error;
            resolve(results)
          });
    })
    console.log(result)
    res.render('second', result[0])
})

app.get('/updatesql', async function(req, res) {
    if(req.query.rank == "") {
        req.query.rank = null
    }

    sql = "UPDATE movies SET name = \'" + req.query.name + "\', year = " + req.query.year + ", `rank` = " + req.query.rank + ", genres = \'" + req.query.genres + "\', director = \'" + req.query.director + "\' where id = " + req.query.id;


    await new Promise ((resolve) => {
        
        connection.query("UPDATE movies SET name = \'" + req.query.name + "\', year = " + req.query.year + ", `rank` = " + req.query.rank + ", genres = \'" + req.query.genres + "\', director = \'" + req.query.director + "\' where id = " + req.query.id, function (error, results, fields) {
            if (error) throw error;
            resolve(results)
          });
    })
    result = await new Promise ((resolve) => {
        connection.query('SELECT * FROM movies WHERE id = ' + req.query.id, function (error, results, fields) {
            if (error) throw error;
            resolve(results)
          });
    })

    
    res.render('second', result[0])
})

app.get('/third', function(req, res) {
    res.render('third')
})

app.get('/insertsql', async function(req, res) {
    if(req.query.rank == "") {
        req.query.rank = null
    }
    
    sql = "INSERT INTO movies(name, year, `rank`, genres, director) values(\'" + req.query.name + "\', " + req.query.year + ", " + req.query.rank + ", \'" + req.query.genres + "\', \'" + req.query.director + "\')"
    
    await new Promise (async (resolve) => {
        
        connection.query("INSERT INTO movies(name, year, `rank`, genres, director) values(\'" + req.query.name + "\', " + req.query.year + ", " + req.query.rank + ", \'" + req.query.genres + "\', \'" + req.query.director + "\')", function (error, results, fields) {
            if (error) resolve(error);
            console.log(results)
            resolve(results)
        });
    })

    result = await new Promise ((resolve) => {
        connection.query('SELECT * FROM movies WHERE name = \'' + req.query.name + "\' and year = " + req.query.year + " and `rank` = " + req.query.rank + " and genres = \'" + req.query.genres + "\' and director = \'" + req.query.director + "\'", function (error, results, fields) {
            if (error) throw error;
            resolve(results)
          });
    })

    sql2 = "INSERT INTO movies(id, name, year, `rank`, genres, director) values(" + result[0].id +", \'" + result[0].name + "\', " + result[0].year + ", " + result[0].rank + ", \'" + result[0].genres + "\', \'" + result[0].director + "\')"

    await new Promise (async (resolve) => {
        if(req.query.year < 1980){
            connection2.query(sql2, function (error, results, fields) {
                if (error) resolve(error)
                else {
                    console.log("Inserting into node 1")
                    resolve(results)
                }
            });
        }
        else {
            connection3.query(sql2, function (error, results, fields) {
                if (error) resolve(error)
                else {
                    console.log("Inserting into node 2")
                    resolve(results)
                }
            });
        }
    });


    res.render('second', result[0])
})

app.get('/deletesql', async function(req, res) {
    console.log("DELETE")

    sql = 'DELETE FROM movies WHERE id = ' + req.query.id

    result = await new Promise ((resolve) => {
        connection.query(sql, function (error, results, fields) {
            if (error) throw error;
            resolve(results)
        });

        if(req.query.year < 1980){
            console.log("Here")
            connection2.query(sql, function (error, results, fields) {
                if (error) resolve(error);
                else {
                    console.log("Deleting from node 1")
                    resolve(results)
                }
            });
        }

        else{
            connection3.query(sql, function (error, results, fields) {
                if (error) resolve(error);
                else {
                    console.log("Deleting from node 2")
                    resolve(results)
                }
            });
        }
    })
    console.log(result)
    res.redirect("/")
})