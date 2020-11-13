const AWS = require('aws-sdk');

AWS.config.logger = console;

const dynamodb = new AWS.DynamoDB.DocumentClient({
  apiVersion: '2012-08-10',
  endpoint: new AWS.Endpoint('http://localhost:8000'),
  region: 'us-west-2',
  // what could you do to improve performance?
});

const tableName = 'SchoolStudents';
const studentLastNameGsiName = 'studentLastNameGsi';

const keyConditionDict = {
  '#schId': ':schId',
  '#stId': ':stId'
};

const attributeNamesDict = {
  '#schId': 'schoolId',
  '#stId': 'studentId'
};

const gsiKeyConditionDict = {
  '#sln': ':sln',
  '#sfn': ':sfn'
};

const gsiAttributeNamesDict = {
  '#sln': 'studentLastName',
  '#sfn': 'studentFirstName'
};

const dynamoLimit = 5;

async function searchByKeySchema({ schoolId, studentId, exclusiveStartKey: ExclusiveStartKey }) {
  const propsDict = { '#schId': schoolId };
  const currentAttributeNamesDict = {};
  const currentValuesDict = {};
  const keyConditionArr = [];
  let attributeValue;

  if (studentId) {
    propsDict['#stId'] = studentId
  }

  for (const key in propsDict) {
    keyConditionArr.push(`${key} = ${keyConditionDict[key]}`);
    attributeValue = keyConditionDict[key];
    currentAttributeNamesDict[key] = attributeNamesDict[key];
    currentValuesDict[attributeValue] = propsDict[key]
  }

  const params = {
    TableName : tableName,
    KeyConditionExpression: keyConditionArr.join(' AND '),
    ExpressionAttributeNames: currentAttributeNamesDict,
    ExpressionAttributeValues: currentValuesDict,
    Limit: dynamoLimit,
    ExclusiveStartKey
  };

  return dynamodb.query(params).promise();
}

async function searchByStudentLastName({ studentLastName, studentFirstName, exclusiveStartKey: ExclusiveStartKey }) {
  const propsDict = { '#sln': studentLastName };
  const currentAttributeNamesDict = {};
  const currentValuesDict = {};
  const keyConditionArr = [];
  let attributeValue;

  if (studentFirstName) {
    propsDict['#sfn'] = studentFirstName
  }

  for (const key in propsDict) {
    keyConditionArr.push(`${key} = ${gsiKeyConditionDict[key]}`);
    attributeValue = gsiKeyConditionDict[key];
    currentAttributeNamesDict[key] = gsiAttributeNamesDict[key];
    currentValuesDict[attributeValue] = propsDict[key]
  }

  const params = {
    TableName : tableName,
    IndexName: studentLastNameGsiName,
    KeyConditionExpression: keyConditionArr.join(' AND '),
    ExpressionAttributeNames: currentAttributeNamesDict,
    ExpressionAttributeValues: currentValuesDict,
    Limit: dynamoLimit,
    ExclusiveStartKey
  };

  return dynamodb.query(params).promise();
}

/**
 * The entry point into the lambda
 *
 * @param {Function} callback
 */
async function fetchAllPages(callback) {
  let dynamoResponse;
  let allResults = [];
  let exclusiveStartKey = null;

  do {
    dynamoResponse = await callback(exclusiveStartKey);
    allResults = [
      ...allResults,
      ...dynamoResponse.Items
    ];
    exclusiveStartKey = dynamoResponse.LastEvaluatedKey;
  } while (exclusiveStartKey);

  return allResults;
}

/**
 * The entry point into the lambda
 *
 * @param {Object} event
 * @param {string} event.schoolId
 * @param {string} event.studentId
 * @param {string} [event.studentLastName]
 */
exports.handler = async ({
  schoolId,
  studentId,
  studentLastName
}) => {
  // TODO use the AWS.DynamoDB.DocumentClient to write a query against the 'SchoolStudents' table and return the results.
  // The 'SchoolStudents' table key is composed of schoolId (partition key) and studentId (range key).

  // TODO (extra credit) if event.studentLastName exists then query using the 'studentLastNameGsi' GSI and return the results.

  // TODO (extra credit) limit the amount of records returned in the query to 5 and then implement the logic to return all
  //  pages of records found by the query (uncomment the test which exercises this functionality)

  let result;

  if (studentLastName) {
    result = await fetchAllPages(
      (exclusiveStartKey) => searchByStudentLastName({ studentLastName, exclusiveStartKey })
    );
  } else {
    result = await fetchAllPages(
      (exclusiveStartKey) => searchByKeySchema({ schoolId, studentId, exclusiveStartKey })
    );
  }

  return result;
};
