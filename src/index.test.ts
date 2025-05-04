import {
  MongoDBContainer,
  StartedMongoDBContainer,
} from "@testcontainers/mongodb";

jest.setTimeout(60_000);

describe("docker", () => {
  let container: StartedMongoDBContainer;

  beforeAll(async () => {
    container = await new MongoDBContainer().start();
  });

  afterAll(async () => {
    await container.stop();
  });

  it("url is not empty", () => {
    const connString = container.getConnectionString();
    expect(connString).toBeTruthy();
  });
});
