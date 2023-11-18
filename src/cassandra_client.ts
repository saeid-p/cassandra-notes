// https://github.com/datastax/nodejs-driver
const cassandra = require("cassandra-driver");
import { readEnvironmentVariable } from "./config";

const host = readEnvironmentVariable("CASSANDRA_HOST") || "localhost";
const port = readEnvironmentVariable("CASSANDRA_PORT") || "9042";
const username = readEnvironmentVariable("CASSANDRA_USERNAME");
const password = readEnvironmentVariable("CASSANDRA_PASSWORD");

const hasCredentials = !!username && !!password;
// Replace 'Username' and 'Password' with the username and password from your cluster settings
const authProvider = hasCredentials ? new cassandra.auth.PlainTextAuthProvider(username, password) : undefined;

const getClient = (keySpace) => {
  const client = new cassandra.Client({
    contactPoints: [`${host}:${port}`],
    localDataCenter: "datacenter1",
    authProvider: authProvider,
    keyspace: keySpace,
  });

  return client;
};

export { getClient };
