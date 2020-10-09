const { DynamoDB } = require('aws-sdk');
const { groupArrayByCount } = require('./helpers');

module.exports = (AwsDocumentClient, awsConfig = {}) => {
  const ClassMethod = AwsDocumentClient || DynamoDB.DocumentClient;
  const docClient = new ClassMethod(awsConfig);

  const maxItemsInBatch = 24;

  docClient.queryAll = async (inParams, inPreviousItems = []) => {
    const { Items, LastEvaluatedKey } = await docClient.query(inParams).promise();

    let items;
    if (LastEvaluatedKey) {
      inParams.ExclusiveStartKey = LastEvaluatedKey;
      items = await docClient.queryAll(inParams, Items);
    } else {
      items = Items;
    }
    return [...items, ...inPreviousItems];
  };

  docClient.scanAll = async (inParams, inPreviousItems = []) => {
    const { Items, LastEvaluatedKey } = await docClient.scan(inParams).promise();
    inParams.ExclusiveStartKey = LastEvaluatedKey;

    let items;
    if (LastEvaluatedKey) {
      inParams.ExclusiveStartKey = LastEvaluatedKey;
      items = await new Promise((resolve, reject) => {
        setTimeout(() => {
          return docClient.scanAll(inParams, Items).then(resolve).catch(reject);
        }, 500);
      });
    } else {
      items = Items;
    }
    return [...items, ...inPreviousItems];
  };

  docClient.batchUpdate = (inTableName, inData = [], inBatchUpdateInterval = 500, inPrimaryKey = null, inSortKey = null) => {
    const groups = groupArrayByCount(inData, maxItemsInBatch * 3);

    return Promise.all(groups.map((groupData) => {
      return batchAction(inTableName, 'PutRequest', inPrimaryKey, inSortKey, groupData, inBatchUpdateInterval);
    }));
  };

  docClient.batchDelete = (inTableName, inPrimaryKey, inSortKey, inData = [], inBatchUpdateInterval = 500) => {
    const groups = groupArrayByCount(inData, maxItemsInBatch * 3);

    return Promise.all(groups.map((groupData) => {
      return batchAction(inTableName, 'DeleteRequest', inPrimaryKey, inSortKey, groupData, inBatchUpdateInterval);
    }));
  };

  /**
   * batchAction
   * @param {*} inTableName
   * @param {*} inRequestAction
   * @param {*} inPrimaryKey
   * @param {*} inSortKey
   * @param {*} inData
   * @param {*} inBatchUpdateInterval
   * @return {Promise}
   */
  async function batchAction(inTableName, inRequestAction, inPrimaryKey, inSortKey, inData, inBatchUpdateInterval = 2000) {
    let startIndex = 0;
    let endIndex = maxItemsInBatch;
    if (endIndex > inData.length) {
      endIndex = inData.length;
    }
    const tasks = [];
    while (endIndex <= inData.length && startIndex !== endIndex) {
      const toModifyData = [];
      for (let index = startIndex; index < endIndex; index++) {
        if (index >= inData.length) {
          break;
        }
        const item = inData[index];
        const modifyRequest = {
          [inRequestAction]: {},
        };
        switch (inRequestAction) {
        case 'PutRequest':
          modifyRequest[inRequestAction].Item = item;
          break;
        default:
          modifyRequest[inRequestAction].Key = {
            [inPrimaryKey]: item[inPrimaryKey],
          };
          break;
        }
        if (inSortKey) {
          modifyRequest[inRequestAction].Key[inSortKey] = item[inSortKey];
        }
        toModifyData.push(modifyRequest);
      }
      const params = {
        RequestItems: {
          [inTableName]: toModifyData,
        },
      };

      startIndex = endIndex;
      endIndex += maxItemsInBatch;
      if (endIndex > inData.length) {
        endIndex = inData.length;
      }
      tasks.push(params);
    }

    const unprocessedParams = [];
    const chainProcess = tasks.reduce((chainPromise, taskParams, index) => {
      return chainPromise
        .then(() => {
          return new Promise((resolve, reject) => {
            const interval = index === 0 ? 0 : inBatchUpdateInterval;
            setTimeout(() => {
              return docClient.batchWrite(taskParams).promise()
                .then(({ UnprocessedItems }) => {
                  if (Object.keys(UnprocessedItems).length > 0) {
                    unprocessedParams.push(UnprocessedItems);
                  }
                  return resolve();
                })
                .catch((err) => {
                  // eslint-disable-next-line
                  return reject({
                    params: taskParams,
                    error: err,
                  });
                });
            }, interval);
          });
        });
    }, Promise.resolve());


    return chainProcess
      .then(() => {
        return handleUnprocessedItems(unprocessedParams, inBatchUpdateInterval);
      });
  }

  function handleUnprocessedItems(inUnprocessed = [], inBatchUpdateInterval) {
    if (inUnprocessed.length === 0) return Promise.resolve();

    const nextUnprocssed = [];

    // try one more time for the unprocessed items
    return Promise.resolve()
      .then(() => {
        return inUnprocessed.reduce((chain, requestParams, index) => {
          return chain
            .then(() => {
              return new Promise((resolve, reject) => {
                const interval = index === 0 ? 0 : inBatchUpdateInterval;
                setTimeout(() => {
                  const params = {
                    RequestItems: requestParams,
                  };
                  return docClient.batchWrite(params).promise()
                    .then(({ UnprocessedItems }) => {
                      if (Object.keys(UnprocessedItems).length > 0) {
                        nextUnprocssed.push(UnprocessedItems);
                      }
                      return resolve();
                    })
                    .catch((err) => {
                      // eslint-disable-next-line
                      return reject({
                        params,
                        error: err,
                      });
                    });
                }, interval);
              });
            });
        }, Promise.resolve());
      })
      .then(() => {
        return handleUnprocessedItems(nextUnprocssed, inBatchUpdateInterval);
      });
  }

  return docClient;
};
