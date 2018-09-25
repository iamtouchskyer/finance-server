const express = require('express');
const mysql   = require('mysql');
const _ = require('lodash');
const axios = require('axios');
const compression = require('compression');

const app = express();

const connection = mysql.createConnection({
  host     : 'localhost',
  user     : 'root',
  password : 'woaitudou',
  database : 'stock',
});

const gStockPrice = {};
let gStocksMetadata; 
doBulkQuery('SELECT * FROM `metadata` ').then((result) => gStocksMetadata = result);

app.use(compression());

app.use(function (req, res, next) {
  // Website you wish to allow to connect
  res.setHeader('Access-Control-Allow-Origin', 'http://localhost:3000');

  // Request methods you wish to allow
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, PATCH, DELETE');

  // Request headers you wish to allow
  res.setHeader('Access-Control-Allow-Headers', 'X-Requested-With,content-type');

  // Set to true if you need the website to include cookies in the requests sent
  // to the API (e.g. in case you use sessions)
  res.setHeader('Access-Control-Allow-Credentials', true);

  // Pass to next layer of middleware
  next();
});

async function doBulkQuery(sqlStatement) {
  return new Promise((resolve, reject) => {
    connection.query(sqlStatement, (error, results, fields) => {
      if (error) reject(error);
      else resolve(results);
    });
  });
}

async function queryLatestPrice(region, symbol) {
  return new Promise((resolve, reject) => {
    if (!gStockPrice[symbol] || (new Date()).getMilliseconds() - gStockPrice[symbol].htime > 30 * 1000) {
      axios.get(`http://hq.sinajs.cn/list=${region.toLowerCase()}${symbol}`)
      .then(response => {
        if (response.status === 200) {
          return response.data;
        } else {
          throw new Error('Something went wrong on api server!');
        }
      })
      .then(response => {
        let results;
            const matched = response.match('"(.*)"');
  
            if (!_.isEmpty(matched)) {
              results = _.zipObject(
                          ['todayOpeningPrice', 'yesterdayClosingPrice', 'currentPrice', 'todayHighestPrice', 'todayLowestPrice'],
                          matched[1].split(',').slice(1, 6)
                        );
            } else {
              console.log(item);
              results = {};
            }

        gStockPrice[symbol] = {data: results, htime: (new Date()).getMilliseconds() };

        resolve(gStockPrice[symbol].data);
      }).catch(error => {
        console.error(error);
        reject(error);
      });
    } else {
      resolve(gStockPrice[symbol].data);
    }
  });
}

app.get('/api/v1/stock/dividend', (req, res) => {
  const region = req.query.region;
  const symbol = req.query.symbol;

  console.log(region + symbol);

  doBulkQuery('SELECT dividend_bonus from `dividend` WHERE symbol=' + symbol + ' AND dividend_year = "2017"')
    .then(result => {
      console.log(result);
      return queryLatestPrice(region, symbol)
              .then((priceResult) => {
                console.log(priceResult);
                console.log(priceResult['currentPrice']);
                console.log(parseFloat(priceResult['currentPrice']));

                console.log(parseFloat(result[0].dividend_bonus) / parseFloat(priceResult['currentPrice']) * 10);
                res.send({dividend_rate : parseFloat(result[0].dividend_bonus) / parseFloat(priceResult['currentPrice']) * 10});
              });
    })
    .catch(error => res.send('0'));
});

app.get('/api/v1/stock/latestprice', (req, res) => {
  const region = req.query.region;
  const symbol = req.query.symbol;

  queryLatestPrice(region, symbol)
    .then(result => res.send(result))
    .catch(error => res.send('0'));
});

app.get('/api/v1/metadata/basic', (req, res) => {
    const result = _.map(gStocksMetadata, (stockMetadata) => {
      return {
        stockName: stockMetadata.name,
        stockSymbol: stockMetadata.symbol,
        stockRegion: stockMetadata.region,
        stockType: stockMetadata.type,
        stockListedAt: stockMetadata.listed_at,
      };
    });

    res.send(result);
});

app.get('/api/v1/stock/detail', (req, res) => {
  const sqlStatements = _.map(['pb', 'pettm', 'petyr', 'totalValue'], (item) => 'SELECT date, value FROM `' + item + '` WHERE symbol = ' + req.query.symbol);

  Promise.all(_.map(sqlStatements, (sqlStatement) => doBulkQuery(sqlStatement)))
    .then((results) => {
      res.send(results);
    });
});

app.get('/api/v1/metadata/advanced', (req, res) => {
  const page = req.query.page;
  const size = req.query.size;
    connection.query('SELECT * FROM `metadata` LIMIT ' + page*size + ', ' + size, function (error, stocksMetadata, fields) {
      const range = '(' + _.map(stocksMetadata, (result) => result.symbol).join(",") + ')';

      Promise.all(
          _.map(['roic', 'roa', 'roe'], (item) => doBulkQuery('SELECT * FROM `' + item + '` WHERE symbol in ' + range))
        )
        .then((rrrResult) => {
          const roicResult = _.groupBy(rrrResult[0], (item) => item.symbol);
          const roaResult = _.groupBy(rrrResult[1], (item) => item.symbol);
          const roeResult = _.groupBy(rrrResult[2], (item) => item.symbol);

          const data = 
            _.map(stocksMetadata, (stockMetadata) => {
              return {
                  stockName: stockMetadata.name,
                  stockSymbol: stockMetadata.symbol,
                  stockRegion: stockMetadata.region,
                  stockType: stockMetadata.type,
                  stockListedAt: stockMetadata.listed_at,
                  roic: 
                    _.zipObject(  
                      _.map(roicResult[stockMetadata.symbol], (item) => item.year),
                      _.map(roicResult[stockMetadata.symbol], (item) => item.value)
                    ),
                  roa:
                    _.zipObject(  
                      _.map(roaResult[stockMetadata.symbol], (item) => item.year),
                      _.map(roaResult[stockMetadata.symbol], (item) => item.value)
                    ),
                  roe:
                    _.zipObject(  
                      _.map(roeResult[stockMetadata.symbol], (item) => item.year),
                      _.map(roeResult[stockMetadata.symbol], (item) => item.value)
                    ),
              };
            });

          res.send(data);
        });
    });
  }
);

app.listen(3030, () => console.log('Example app listening on port 3030!'))
