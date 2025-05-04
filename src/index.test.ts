import {
  MongoDBContainer,
  StartedMongoDBContainer,
} from "@testcontainers/mongodb";

let container: StartedMongoDBContainer;

beforeAll(async () => {
  container = await new MongoDBContainer().start();
});

afterAll(async () => {
  await container.stop();
});

describe("makeClient", () => {
  it("url is not empty", () => {
    const connString = container.getConnectionString();
    console.log(connString);
    expect(connString).toBeTruthy();
  });
});
