import { describe, test, expect, afterAll, beforeAll } from '@jest/globals';
import { ReliableRedisClient } from './reliableRedisClient';
import { Redis } from 'ioredis';

// There are extensive tests in the ioredis-mock repo, but this just does a few basic functionality tests to prove the jest config works.

describe("Test Redis Mocking", () => {
    let rrc: ReliableRedisClient;
    let client: Redis;

    beforeAll(async () => {
        rrc = new ReliableRedisClient("Test", "localhost", 6379);
        await rrc.connect();
        client = await rrc.getClient();
        await client.del("test");
        await client.del("list");
    })

    test("Setting a key", async () => {
        await client.set("test", "TEST");
        expect(await client.get("test")).toEqual("TEST");
    });

    test("Overwriting a key", async () => {
        await client.set("test", "TEST2");
        expect(await client.get("test")).toEqual("TEST2");
        await client.set("test", "TEST3");
        expect(await client.get("test")).toEqual("TEST3");
    });

    test("Setting a set", async () => {
        await client.sadd("list", ['one', 'two']);
        expect(new Set(await client.smembers("list"))).toEqual(new Set(['one', 'two']));
        await client.sadd("list", ['three', 'four']);
        expect(new Set(await client.smembers("list"))).toEqual(new Set(['one', 'two', 'three', 'four']));
    });

    afterAll(async () => {
        await rrc.shutdown();
    });
})