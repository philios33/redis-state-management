import { describe, test, expect, afterAll, beforeAll } from '@jest/globals';
import { ReliableRedisClient } from '../reliableRedisClient';
import { RedisQueuesController } from './controller';
import { GenericMessage } from '../types';

describe("Test Queue Controller", () => {
    let rrc: ReliableRedisClient;
    let rqc: RedisQueuesController;

    beforeAll(async () => {
        rrc = new ReliableRedisClient("Test", "localhost", 6379);
        await rrc.connect();
        rqc = new RedisQueuesController(rrc, "test");
    })

    test("Pushing and popping to a queue", async () => {
        const queueId = "TEST1";
        await rqc.deleteQueue(queueId);

        const example: GenericMessage = {
            type: "Example",
            meta: {
                isAnExample: true
            },
            occurredAt: new Date().toISOString(),
        }
        await rqc.pushMessage(queueId, example);
        const size = await rqc.getQueueSize(queueId);
        expect(size).toEqual(1);

        const retrieved = await rqc.popNextMessage(queueId);
        expect(retrieved).not.toEqual(null);
        if (retrieved === null) { 
            throw new Error("Retreived should not equal null");
        }
        
        expect(retrieved.message).toEqual(example);
        const size2 = await rqc.getQueueSize(queueId);
        expect(size2).toEqual(0);

        await rqc.confirmMessageById(queueId, retrieved.id);
        const size3 = await rqc.getQueueSize(queueId);
        expect(size3).toEqual(0);

        const retrieved2 = await rqc.popNextMessage(queueId);
        expect(retrieved2).toEqual(null); // Empty list
    });

    test("Trying to pop without confirmation retreives the same message over and over", async () => {
        const queueId = "TEST2";
        await rqc.deleteQueue(queueId);
        const example: GenericMessage = {
            type: "Example2",
            meta: {
                isAnExample: true
            },
            occurredAt: new Date().toISOString(),
        }
        await rqc.pushMessage(queueId, example);
        const size = await rqc.getQueueSize(queueId);
        expect(size).toEqual(1);

        const retrieved = await rqc.popNextMessage(queueId);
        expect(retrieved).not.toEqual(null);
        if (retrieved === null) { 
            throw new Error("Retreived should not equal null");
        }
        expect(retrieved.message).toEqual(example);

        // Same result before confirmation
        const retrieved2 = await rqc.popNextMessage(queueId);
        expect(retrieved2).not.toEqual(null);
        if (retrieved2 === null) { 
            throw new Error("Retreived2 should not equal null");
        }
        expect(retrieved2.message).toEqual(example);
    });

    test("Confirming the same message more than once throws an error", async () => {
        const queueId = "TEST3";
        await rqc.deleteQueue(queueId);
        const example: GenericMessage = {
            type: "Example3",
            meta: {
                isAnExample: true
            },
            occurredAt: new Date().toISOString(),
        }
        await rqc.pushMessage(queueId, example);
        const size = await rqc.getQueueSize(queueId);
        expect(size).toEqual(1);

        const retrieved = await rqc.popNextMessage(queueId);
        expect(retrieved).not.toEqual(null);
        if (retrieved === null) { 
            throw new Error("Retreived should not equal null");
        }
        expect(retrieved.message).toEqual(example);

        await rqc.confirmMessageById(queueId, retrieved.id);

        await expect(async () => {
            await rqc.confirmMessageById(queueId, retrieved.id);
        }).rejects.toThrowError(/^Could not confirm message:/);
    });

    test("Hang connection using pub/sub", async () => {
        const queueId = "TEST4";
        await rqc.deleteQueue(queueId);

        // Push after 2 seconds
        setTimeout(() => {
            const example: GenericMessage = {
                type: "Example4",
                meta: {
                    isAnExample: true
                },
                occurredAt: new Date().toISOString(),
            }
            rqc.pushMessage(queueId, example);
        }, 2000);

        const start = new Date().getTime();
        const control = {
            isCancelled: false
        }
        await rqc.hangConnectionUntilNextMessageOrCancelled(queueId, control);
        const stop = new Date().getTime();
        const hungFor = stop - start;
        // console.log("HUNG FOR", hungFor);
        expect(hungFor).toBeGreaterThan(2000);
        expect(hungFor).toBeLessThan(3000);

    })
    

    afterAll(async () => {
        await rrc.shutdown();
    });
})