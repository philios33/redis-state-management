// Decided to just use a local stateless redis for testing instead
// jest.mock('ioredis', () => jest.requireActual('ioredis-mock'));

jest.setTimeout(30000); // in milliseconds