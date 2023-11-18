import * as Cassandra from "../cassandra_client";
const cassandra = require("cassandra-driver");

const KEYSPACE_NAME = "test_keyspace_1";
const TABLE_NAME = "Test_Table1";

describe("MongoDb CRUD basic commands.", () => {
  let client;

  beforeAll(() => (client = Cassandra.getClient(KEYSPACE_NAME)));
  afterAll(() => client?.shutdown());

  it("Should create a keyspace.", async () => {
    const systemClient = Cassandra.getClient("system");
    const command = `
    CREATE KEYSPACE IF NOT EXISTS ${KEYSPACE_NAME}
    WITH replication = {
      'class': 'SimpleStrategy',
      'replication_factor': 1
    };`;

    const response = await systemClient.execute(command);

    expect(response).toBeTruthy();
    systemClient?.shutdown();
  });

  it("Should create a table.", async () => {
    const command = `
    CREATE TABLE IF NOT EXISTS ${KEYSPACE_NAME}.${TABLE_NAME} (
      id UUID PRIMARY KEY,
      name TEXT,
      age INT
    );`;

    const response = await client.execute(command);

    expect(response).toBeTruthy();
  });

  it("Should add a row to a table.", async () => {
    const command = `INSERT INTO ${KEYSPACE_NAME}.${TABLE_NAME} (id, name, age) VALUES (?, ?, ?);`;
    const id = cassandra.types.Uuid.random();
    const name = "John Doe";
    const age = 30;

    const response = await client.execute(command, [id, name, age], { prepare: true });

    expect(response).toBeTruthy();
  });

  it("Should find a row in a table.", async () => {
    const query = `SELECT id, name FROM ${KEYSPACE_NAME}.${TABLE_NAME} WHERE id = ?`;
    const id = "d9c8c09b-162b-4888-aff9-ef28f46ebf8d";

    const response = await client.execute(query, [id], { prepare: true });
    const row = response.first();

    expect(row).toBeTruthy();
    expect(row.id.toString()).toBe(id);
    expect(row.name).toBeTruthy();
    expect(row.age).toBeFalsy();
  });
});
