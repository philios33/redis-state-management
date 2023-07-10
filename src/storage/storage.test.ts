// Integration test of the combination of these 3 classes
// Any writes should be instantaneous if the processor is operating correctly

import { describe, test, expect, afterAll, beforeAll } from '@jest/globals';
import { ReliableRedisClient } from '../reliableRedisClient';
import { DiffMessage, GenericMessage, StateVersion } from '../types';
import { RedisStorageStateReader } from './stateReader';
import { RedisStorageStateWriter } from './stateWriter';
import { RedisStorageProcessor } from './processor';
import { RedisQueuesController } from '../queues/controller';

describe("Test Storage Integration", () => {
    let rrc: ReliableRedisClient;
    let reader: RedisStorageStateReader;
    let writer: RedisStorageStateWriter;
    let processor: RedisStorageProcessor;

    beforeAll(async () => {
        rrc = new ReliableRedisClient("Test", "localhost", 6379);
        await rrc.connect();

        const namespace = "TEST";
        const processingQueueId = "PROCESSING";

        // This is necessary to remove any historic testing from the persistent redis
        const tempQueues = new RedisQueuesController(rrc, namespace);
        await tempQueues.deleteQueue(processingQueueId);

        // Also we need to remove the lock key from previous tests
        const lockKey = "STORAGE_PROCESSOR_" + namespace;
        (await rrc.getClient()).del(lockKey);

        reader = new RedisStorageStateReader(rrc, namespace);
        writer = new RedisStorageStateWriter(rrc, namespace, processingQueueId);
        processor = new RedisStorageProcessor(rrc, namespace, processingQueueId);
        await processor.start();
    });

    test("Setting a single value", async () => {
        await writer.setValue("test", "TEST2");
        await processor.sleep(500);
        const value = await reader.getValue("test");
        expect(value).toEqual("TEST2");
    });

    test("Setting a single object", async () => {
        const obj = {
            phil: "test",
            true: true,
        }
        await writer.setValue("test2", obj);
        await processor.sleep(500);
        const value = await reader.getValue("test2");
        expect(value).toEqual(obj);
    });

    test("Putting some values in a hashmap", async () => {
        await writer.setHashmapValue("hmap1", "phil", "great");
        await writer.setHashmapValue("hmap1", "someBool", true);
        await writer.setHashmapValue("hmap1", "number", 24);
        await writer.setHashmapValue("hmap1", "obj", {
            everything: "here",
            at: 1,
            time: true
        });
        await processor.sleep(500);
        const size = await reader.getHashmapSize("hmap1");
        expect(size).toEqual(4);

        const philValue = await reader.getHashmapValue("hmap1", "phil");
        expect(philValue).toEqual("great");

        const allValues = await reader.getAllHashmapValues("hmap1");
        expect(allValues).toEqual(["great", true, 24, {
            everything: "here",
            at: 1,
            time: true
        }]);

        const value = await reader.getHashmapAsRecord<string | boolean | number | object>("hmap1");
        expect(value).toEqual({
            phil: "great",
            number: 24,
            obj: {
                at: 1,
                time: true,
                everything: "here",
            },
            someBool: true
        });
    });

    test("Putting some string values in a set", async () => {
        await writer.addToStringSet("set1", ['one','two']);
        await writer.addToStringSet("set1", ['three','four']);
        await processor.sleep(500);
        const value = await reader.getStringSet("set1");
        expect(new Set(value)).toEqual(new Set(['one','two','three','four']));
    });

    test("Setting a state object and listen for changes", async () => {
        await writer.removeStateObj("test3"); // Reset value
        const obj = {
            phil: "test",
            stage: 1,
        }
        await writer.writeStateObj("test3", obj);
        await processor.sleep(500);
        const value = await reader.readStateObj<any>("test3");
        expect(value).not.toEqual(null);
        if (value === null) {
            throw new Error("Value should not be null");
        }
        expect(value.value).toEqual(obj);
        expect(value.version).toEqual(1);

        obj.stage = 2;
        await writer.writeStateObj("test3", obj);
        await processor.sleep(500);
        const value2 = await reader.readStateObj<any>("test3");
        expect(value2).not.toEqual(null);
        if (value2 === null) {
            throw new Error("Value should not be null");
        }
        expect(value2.value).toEqual(obj);
        expect(value2.version).toEqual(2);

        const fullCallbacks: Array<StateVersion<any>> = [];
        const deltaCallbacks: Array<DiffMessage> = [];

        const unsubscribe = await reader.fetchStateAndListen<any>("test3", (full: any) => {
            fullCallbacks.push(full);
        }, (delta: any) => {
            deltaCallbacks.push(delta);
        });

        await processor.sleep(1000);

        expect(fullCallbacks.map(fcb => ({...fcb, writtenAt: null}))).toEqual([{
            version: 2,
            value: {
                phil: "test",
                stage: 2,
            },
            writtenAt: null,
        }]);
        expect(deltaCallbacks).toEqual([]);

        await processor.sleep(1000);

        obj.stage = 3;
        await writer.writeStateObj("test3", obj);

        await processor.sleep(1000);
        expect(fullCallbacks.map(fcb => ({...fcb, writtenAt: null}))).toEqual([{
            version: 2,
            value: {
                phil: "test",
                stage: 2,
            },
            writtenAt: null,
        }]);
        expect(deltaCallbacks.length).toEqual(1);
        expect(deltaCallbacks[0].fromVersion).toEqual(2);
        expect(deltaCallbacks[0].toVersion).toEqual(3);
        
        unsubscribe();
        
    });

    afterAll(async () => {
        await processor.stop();
        rrc.shutdown();
    });

});