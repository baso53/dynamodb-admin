const express = require('express');
const path = require('path');
const errorhandler = require('errorhandler');
const {
  extractKey,
  extractKeysForItems,
  parseKey,
  doSearch
} = require('./util');
const asyncMiddleware = require('./utils/asyncMiddleware');
const bodyParser = require('body-parser');
const pickBy = require('lodash.pickby');

exports.createServer = (dynamodb, docClient, config) => {
  const app = express();
  app.set('json spaces', 2);
  app.set('view engine', 'ejs');
  app.set('views', path.resolve(__dirname, '..', 'views'));

  const describeTable = (...args) => dynamodb.describeTable(...args).promise();
  const getItem = (...args) => docClient.get(...args).promise();
  const putItem = (...args) => docClient.put(...args).promise();
  const deleteItem = (...args) => docClient.delete(...args).promise();

  app.use(errorhandler());
  app.use('/assets', express.static(path.join(__dirname, '..', 'public')));

  app.get('/', (req, res) => {
    if (!dynamodb || !docClient) {
      res.render('home');
      return;
    }

    res.redirect(`/tables/${config.table}/get`);
  });

  app.post(
    '/',
    bodyParser.urlencoded({ extended: true }),
    asyncMiddleware((req, res) => {
      if (!dynamodb || !docClient || !config) {
        const AWS = require('aws-sdk');

        if (!dynamodb) {
          dynamodb = new AWS.DynamoDB(req.body);
        }

        docClient =
          docClient || new AWS.DynamoDB.DocumentClient({ service: dynamodb });

        config = req.body;
      }

      res.redirect('/');
    })
  );

  app.get(
    '/tables/:TableName/get',
    asyncMiddleware((req, res) => {
      const TableName = req.params.TableName;
      if (req.query.hash) {
        if (req.query.range) {
          return res.redirect(
            `/tables/${encodeURIComponent(
              TableName
            )}/items/${encodeURIComponent(req.query.hash)},${encodeURIComponent(
              req.query.range
            )}`
          );
        } else {
          return res.redirect(
            `/tables/${encodeURIComponent(
              TableName
            )}/items/${encodeURIComponent(req.query.hash)}`
          );
        }
      }

      return describeTable({ TableName }).then(description => {
        const hashKey = description.Table.KeySchema.find(schema => {
          return schema.KeyType === 'HASH';
        });
        if (hashKey) {
          hashKey.AttributeType = description.Table.AttributeDefinitions.find(
            definition => {
              return definition.AttributeName === hashKey.AttributeName;
            }
          ).AttributeType;
        }
        const rangeKey = description.Table.KeySchema.find(schema => {
          return schema.KeyType === 'RANGE';
        });
        if (rangeKey) {
          rangeKey.AttributeType = description.Table.AttributeDefinitions.find(
            definition => {
              return definition.AttributeName === rangeKey.AttributeName;
            }
          ).AttributeType;
        }
        res.render(
          'get',
          Object.assign({}, description, {
            hashKey,
            rangeKey
          })
        );
      });
    })
  );

  const getPage = (
    docClient,
    keySchema,
    TableName,
    scanParams,
    pageSize,
    startKey,
    operationType
  ) => {
    const pageItems = [];

    function onNewItems(items, lastStartKey) {
      for (
        let i = 0;
        i < items.length && pageItems.length < pageSize + 1;
        i++
      ) {
        pageItems.push(items[i]);
      }

      // If there is more items to query (!lastStartKey) then don't stop until
      // we are over pageSize count. Stopping at exactly pageSize count would
      // not extract key of last item later and make pagination not work.
      return pageItems.length > pageSize || !lastStartKey;
    }

    return doSearch(
      docClient,
      TableName,
      scanParams,
      10,
      startKey,
      onNewItems,
      operationType
    ).then(items => {
      let nextKey = null;

      if (items.length > pageSize) {
        items = items.slice(0, pageSize);
        nextKey = extractKey(items[pageSize - 1], keySchema);
      }

      return {
        pageItems: items,
        nextKey
      };
    });
  };

  app.get(
    '/tables/:TableName',
    asyncMiddleware((req, res) => {
      const TableName = req.params.TableName;
      req.query = pickBy(req.query);

      return describeTable({ TableName }).then(description => {
        const pageNum = req.query.pageNum ? parseInt(req.query.pageNum) : 1;

        const data = Object.assign({}, description, {
          query: req.query,
          pageNum,
          operators: {
            '=': '=',
            '<>': '≠',
            '>=': '>=',
            '<=': '<=',
            '>': '>',
            '<': '<'
          },
          attributeTypes: {
            S: 'String',
            N: 'Number'
          }
        });
        res.render('scan', data);
      });
    })
  );

  app.get(
    '/tables/:TableName/items',
    asyncMiddleware((req, res) => {
      const TableName = req.params.TableName;
      req.query = pickBy(req.query);
      const filters = req.query.filters ? JSON.parse(req.query.filters) : {};

      return describeTable({ TableName }).then(description => {
        const ExclusiveStartKey = req.query.startKey
          ? JSON.parse(req.query.startKey)
          : {};
        const pageNum = req.query.pageNum ? parseInt(req.query.pageNum) : 1;
        const ExpressionAttributeNames = {};
        const ExpressionAttributeValues = {};
        const FilterExpressions = [];
        const KeyConditions = [];
        const KeyConditionExpression = [];
        const queryableSelection = req.query.queryableSelection || 'table';
        let indexBeingUsed = null;

        if (req.query.operationType === 'query') {
          if (queryableSelection === 'table') {
            indexBeingUsed = description.Table;
          } else if (description.Table.GlobalSecondaryIndexes) {
            indexBeingUsed = description.Table.GlobalSecondaryIndexes.find(
              index => {
                return index.IndexName === req.query.queryableSelection;
              }
            );
          }
        }

        for (const key in filters) {
          if (filters[key].type === 'N') {
            filters[key].value = Number(filters[key].value);
          }
          ExpressionAttributeNames[`#${key}`] = key;
          ExpressionAttributeValues[`:${key}`] = filters[key].value;
          if (
            indexBeingUsed &&
            indexBeingUsed.KeySchema.find(
              keySchemaItem => keySchemaItem.AttributeName === key
            )
          ) {
            KeyConditionExpression.push(
              `#${key} ${filters[key].operator} :${key}`
            );
          } else {
            ExpressionAttributeNames[`#${key}`] = key;
            ExpressionAttributeValues[`:${key}`] = filters[key].value;
            FilterExpressions.push(`#${key} ${filters[key].operator} :${key}`);
          }
        }

        const params = pickBy({
          TableName,
          FilterExpression: FilterExpressions.length
            ? FilterExpressions.join(' AND ')
            : undefined,
          ExpressionAttributeNames: Object.keys(ExpressionAttributeNames).length
            ? ExpressionAttributeNames
            : undefined,
          ExpressionAttributeValues: Object.keys(ExpressionAttributeValues)
            .length
            ? ExpressionAttributeValues
            : undefined,
          KeyConditions: Object.keys(KeyConditions).length
            ? KeyConditions
            : undefined,
          KeyConditionExpression: KeyConditionExpression.length
            ? KeyConditionExpression.join(' AND ')
            : undefined
        });

        if (
          req.query.queryableSelection &&
          req.query.queryableSelection !== 'table'
        ) {
          params.IndexName = req.query.queryableSelection;
        }

        const startKey = Object.keys(ExclusiveStartKey).length
          ? ExclusiveStartKey
          : undefined;

        return getPage(
          docClient,
          description.Table.KeySchema,
          TableName,
          params,
          25,
          startKey,
          req.query.operationType
        )
          .then(results => {
            const { pageItems, nextKey } = results;

            const nextKeyParam = nextKey
              ? encodeURIComponent(JSON.stringify(nextKey))
              : null;

            const primaryKeys = description.Table.KeySchema.map(
              schema => schema.AttributeName
            );
            // Primary keys are listed first.
            const uniqueKeys = [
              ...primaryKeys,
              ...extractKeysForItems(pageItems).filter(
                key => !primaryKeys.includes(key)
              )
            ];

            // Append the item key.
            for (const item of pageItems) {
              item.__key = extractKey(item, description.Table.KeySchema);
            }

            const data = Object.assign({}, description, {
              query: req.query,
              pageNum,
              prevKey: encodeURIComponent(req.query.prevKey || ''),
              startKey: encodeURIComponent(req.query.startKey || ''),
              nextKey: nextKeyParam,
              filterQueryString: encodeURIComponent(req.query.filters || ''),
              Items: pageItems,
              uniqueKeys
            });

            res.json(data);
          })
          .catch(error => {
            res
              .status(400)
              .send(
                (error.code ? '[' + error.code + '] ' : '') + error.message
              );
          });
      });
    })
  );

  app.get(
    '/tables/:TableName/meta',
    asyncMiddleware((req, res) => {
      const TableName = req.params.TableName;
      return Promise.all([
        describeTable({ TableName }),
        docClient.scan({ TableName }).promise()
      ]).then(([description, items]) => {
        const data = Object.assign({}, description, items);
        res.render('meta', data);
      });
    })
  );

  app.delete(
    '/tables/:TableName/items/:key',
    asyncMiddleware((req, res) => {
      const TableName = req.params.TableName;
      return describeTable({ TableName }).then(result => {
        const params = {
          TableName,
          Key: parseKey(req.params.key, result.Table)
        };

        return deleteItem(params).then(() => {
          res.status(204).end();
        });
      });
    })
  );

  app.get(
    '/tables/:TableName/add-item',
    asyncMiddleware((req, res) => {
      const TableName = req.params.TableName;
      return describeTable({ TableName }).then(result => {
        const table = result.Table;
        const Item = {};
        table.KeySchema.forEach(key => {
          const definition = table.AttributeDefinitions.find(attribute => {
            return attribute.AttributeName === key.AttributeName;
          });
          Item[key.AttributeName] = definition.AttributeType === 'S' ? '' : 0;
        });
        res.render('item', {
          Table: table,
          TableName: req.params.TableName,
          Item: Item,
          isNew: true
        });
      });
    })
  );

  app.get(
    '/tables/:TableName/items/:key',
    asyncMiddleware((req, res) => {
      const TableName = req.params.TableName;
      return describeTable({ TableName }).then(result => {
        const params = {
          TableName,
          Key: parseKey(req.params.key, result.Table)
        };

        return getItem(params).then(response => {
          if (!response.Item) {
            return res.status(404).send('Not found');
          }
          res.render('item', {
            Table: result.Table,
            TableName: req.params.TableName,
            Item: response.Item,
            isNew: false
          });
        });
      });
    })
  );

  app.put(
    '/tables/:TableName/add-item',
    bodyParser.json({ limit: '500kb' }),
    asyncMiddleware((req, res) => {
      const TableName = req.params.TableName;
      return describeTable({ TableName }).then(description => {
        const params = {
          TableName,
          Item: req.body
        };

        return putItem(params).then(() => {
          const Key = extractKey(req.body, description.Table.KeySchema);
          const params = {
            TableName,
            Key
          };
          return getItem(params).then(response => {
            if (!response.Item) {
              return res.status(404).send('Not found');
            }
            return res.json(Key);
          });
        });
      });
    })
  );

  app.put(
    '/tables/:TableName/items/:key',
    bodyParser.json({ limit: '500kb' }),
    asyncMiddleware((req, res) => {
      const TableName = req.params.TableName;
      return describeTable({ TableName }).then(result => {
        const params = {
          TableName,
          Item: req.body
        };

        return putItem(params).then(() => {
          const params = {
            TableName,
            Key: parseKey(req.params.key, result.Table)
          };
          return getItem(params).then(response => {
            return res.json(response.Item);
          });
        });
      });
    })
  );

  app.use((err, req, res, next) => {
    console.error(err);
    next(err);
  });

  return app;
};
