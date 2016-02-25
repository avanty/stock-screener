var fs = require("fs");
var http = require('http');
var sqlite3 = require("sqlite3").verbose();

var file = "stocks.db3";
var db = new sqlite3.Database(file);

// pool 股票池
var pool = { default: "peg" };

pool.pegGet = function (perpage, index) {
    var options = {
        hostname: 'www.iwencai.com',
        path: '/wap/search?source=phone&w=peg%3E0%E4%B8%94%3C6,pe%3E0%E4%B8%94%3C200,%E6%89%80%E5%B1%9E%E6%A6%82%E5%BF%B5&perpage=' + perpage + '&p=' + index,
        method: 'GET'
    };
    var req = http.request(options, function (res) {
        console.log('GET page:' + index + " " + res.statusCode);
        //console.log('HEADERS: ' + JSON.stringify(res.headers));
        //res.setEncoding('utf8');
        var body = '';
        res.on('data', function (chunk) {
            body += chunk;
        });
        res.on('end', function () {
            var xuangu = JSON.parse(body).xuangu.blocks[0].data;
            //console.dir(xuangu);
            if (index == 1) {
                pool.totalPage = xuangu.total / perpage;
                // code text, name text, peg real, pe real, pb real, capital real, concepts integer, industry text
                pool.columns = [];
                for (var col in xuangu.indexID) {
                    // console.log(col + ": " + xuangu.indexID[col]);
                    if (xuangu.indexID[col].indexOf("code") >= 0) {
                        pool.columns[0] = col;
                    } else if (xuangu.indexID[col].indexOf("name") >= 0) {
                        pool.columns[1] = col;
                    } else if (xuangu.indexID[col].indexOf("历史peg") >= 0) {
                        pool.columns[2] = col;
                    } else if (xuangu.indexID[col].indexOf("(pe)") >= 0) {
                        pool.columns[3] = col;
                    } else if (xuangu.indexID[col].indexOf("(pb)") >= 0) {
                        pool.columns[4] = col;
                    } else if (xuangu.indexID[col].indexOf("总股本") >= 0) {
                        pool.columns[5] = col;
                    } else if (xuangu.indexID[col].indexOf("概念数") >= 0) {
                        pool.columns[6] = col;
                    } else if (xuangu.indexID[col].indexOf("行业") >= 0) {
                        pool.columns[7] = col;
                    }
                }
            }
            pool.save(xuangu.result);
            if (index <= pool.totalPage) {
                pool.pegGet(perpage, index + 1);
            } else {
                db.close();
            }
        })
    });

    req.on('error', function (e) {
        console.log('problem with request: ' + e.message);
    });

    // write data to request body
    req.end();
}

pool.peg = function () {
    pool.pegGet(20, 1);
}

pool.save = function (rows) {
    db.serialize(function () {
        db.run("create table if not exists pools (code text, name text, peg real, pe real, pb real, capital real, concepts integer, industry text)");

        var stmt = db.prepare("insert into pools values (?, ?, ?, ?, ?, ?, ?, ?)");
        for (var i = 0, len = rows.length; i < len; i++) {
            var row = rows[i];
            stmt.run([row[pool.columns[0]].substring(0, 6), row[pool.columns[1]], pool.fixed(row[pool.columns[2]]),
                pool.fixed(row[pool.columns[3]]), pool.fixed(row[pool.columns[4]]), pool.fixed(row[pool.columns[5]] / 100000000.0),
                row[pool.columns[6]], row[pool.columns[7]]]);
        }
        stmt.finalize();

        //db.each("select rowid as rowid, name from pools", function (err, row) {
        //    console.log(row.rowid + ": " + row.thing);
        //});
    });
}

pool.fixed = function (num) {
    return new Number(num).toFixed(2);
}

function inArray(elem, array) {
    for (var i = 0, len = array.length; i < len; i++) {
        if (array[i] == elem) {
            return i;
        }
    }
    return -1;
}

// 行情数据 http://q.stock.sohu.com/hisHq?code=cn_300058&start=20160215&order=A
var kline = { default: "downAll", codes: [], okCodes: [], errCodes: [], saveCodes: [] };

kline.downAll = function (start) {
    db.serialize(function () {
        var codes = [];
        db.each("select code from pools", function (err, row) {
            codes.push(row.code);
        }, function () {
            console.log('... ... ... downAll ' + codes.length);
            db.run("BEGIN"); // 开启事务，否则很慢
            kline.downMany(codes, start);
        });
    });
}

kline.downMany = function (codes, start) {
    kline.codes = codes;
    kline.okCodes = [];
    kline.errCodes = [];
    kline.saveCodes = [];
    for (var i = 0, len = kline.codes.length; i < len; i++) {
        (function (index) {
            setTimeout(function () {
                console.log("down " + kline.codes[index]);
                kline.downOne(kline.codes[index], start);
            }, 300 * (index + 1));
        })(i);
    }
}

kline.downOne = function (code, start) {
    var path = '/hisHq?code=cn_' + code;
    if (start) {
        path += '&order=A&start=' + start;
    }
    var options = {
        hostname: 'q.stock.sohu.com',
        path: path,
        method: 'GET'
    };
    var req = http.request(options, function (res) {
        console.log('resp ' + code + ": " + res.statusCode);
        //console.log('HEADERS: ' + JSON.stringify(res.headers));
        //res.setEncoding('utf8');
        var body = '';
        res.on('data', function (chunk) {
            body += chunk;
        });
        res.on('end', function () {
            var status = JSON.parse(body)[0].status;
            var hq = JSON.parse(body)[0].hq;
            if (status == 0) {
                kline.okCodes.push(code);
            } else {
                console.log('resp ' + code + ', status ' + status);
                if (inArray(code, kline.errCodes) == -1) {
                    kline.errCodes.push(code);
                } else {
                    console.log('??? ' + code + ' already in err codes ???');
                }
            }
            kline.save(code, hq);
            kline.progress(start);
        })
    });

    req.on('error', function (e) {
        console.log('resp ' + code + " error, " + e.message);
        if (inArray(code, kline.errCodes) == -1) {
            kline.errCodes.push(code);
        } else {
            console.log('??? ' + code + ' already in err codes ???');
        }
        kline.progress(start);
    });

    // write data to request body
    req.end();
}

kline.progress = function (start) {
    console.log('... ... ... all: ' + kline.codes.length + ', ok: ' + kline.okCodes.length + ', err: ' + kline.errCodes.length +
        ', save: ' + kline.saveCodes.length)
    if (kline.okCodes.length + kline.errCodes.length >= kline.codes.length) {
        if (kline.errCodes.length > 0) {
            console.log('... ... ... retry ' + kline.errCodes.length);
            kline.downMany(kline.errCodes, start);
        } else {
            //console.log('... ... ... down finish !');
            //db.run("COMMIT"); db save并未结束
        }
    }
}

kline.save = function (code, rows) {
    db.serialize(function () {
        db.run("create table if not exists klines (code text, date text, open real, close real, low real, high real, hsl real)");

        var stmt = db.prepare("insert into klines values (?, ?, ?, ?, ?, ?, ?)");

        var effectedCount = 0;
        for (var i = 0, len = rows.length; i < len; i++) {
            var row = rows[i];
            stmt.run([code, row[0], row[1], row[2], row[5], row[6], row[9]], function (e) {
                if (e) {
                    console.log('Save code:' + code + " error, " + e.message);
                }
                effectedCount++;
                if (effectedCount == rows.length) {
                    kline.saveCodes.push(code);
                    if (kline.saveCodes.length >= kline.codes.length) {
                        console.log('... ... ... all: ' + kline.codes.length + ', ok: ' + kline.okCodes.length + ', err: ' +
                            kline.errCodes.length + ', save: ' + kline.saveCodes.length)
                        console.log('... ... ... finish !');
                        db.run('COMMIT');
                    }
                }
            });
        }
        //db.run("COMMIT");
        stmt.finalize();
    });
}

var screen = { default: "all" };


/*
db.serialize(function () {
    if (!exists) {
        db.run("create table if not exists pools (code text, name text, peg real, pe real, pb real, capital real, concepts integer)");
    }

    var stmt = db.prepare("insert into stuff values (?)");
    var rnd;
    for (var i = 0; i < 1; i++) {
        rnd = Math.floor(Math.random() * 10000000);
        stmt.run("thing #" + rnd);
    }

    stmt.finalize();

    db.each("select rowid as rowid, thing from stuff", function (err, row) {
        //console.log(row.rowid + ": " + row.thing);
    });

});


db.close();
*/


var act = process.argv[2] || "screen";
var actArgv = process.argv.slice(3);
var caller = act === "pool" ? pool : act === "kline" ? kline : screen;
var method = actArgv.length ? actArgv[0] : caller.default;
console.dir(caller);
var fn = caller[method];
if (typeof fn === "function") {
    fn.apply(caller);
} else {
    console.error("no such method");
}