import {
  MongoDBContainer,
  StartedMongoDBContainer,
} from "@testcontainers/mongodb";
import { Client, makeClient } from ".";

jest.setTimeout(60_000);

describe("docker", () => {
  let container: StartedMongoDBContainer;
  let client: Client;

  beforeAll(async () => {
    container = await new MongoDBContainer().start();
    const url = container.getConnectionString();
    client = await makeClient(url, new Map());
  });

  afterAll(async () => {
    await client.close();
    await container.stop();
  });

  it("url is not empty", () => {
    const connString = container.getConnectionString();
    expect(connString).toBeTruthy();
  });
});
