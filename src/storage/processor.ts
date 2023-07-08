

// This is just a thread that does the processing work.  
// Make sure you only run one of these at a time
// This processor will die with process.exit(1) if there are issues

import { Redis } from "ioredis";
import { RedisQueuesController } from "../queues/controller";
import { ReliableRedisClient } from "../reliableRedisClient";
import { v4 as uuidv4 } from "uuid";

export default class RedisStorageProcessor {
    redis: Redis;
    namespace: string;
    queues: RedisQueuesController;
    incomingQueueId: string;
    instanceId: string;
    isPaused: boolean;
    isProcessing: boolean;

    constructor(rrc: ReliableRedisClient, namespace: string, incomingQueueId: string) {
        this.redis = rrc.getClient();
        this.namespace = namespace;

        this.queues = new RedisQueuesController(rrc, namespace);
        this.incomingQueueId = incomingQueueId;
        this.instanceId = uuidv4();
        this.isPaused = false;
        this.isProcessing = false;
    }

    async start() {
        // We attempt a crude lock mechanism by waiting for a variable to be null in redis
        const lockKey = "STORAGE_PROCESSOR_" + this.namespace;
        const maxAttempts = 10;
        const waitingSeconds = 10;
        let attempt = 0;
        let value;
        do {
            attempt++;
            if (attempt > maxAttempts) {
                console.error("Maximum locking attempts exceeded: " + maxAttempts);
                process.exit(1);
            }
            value = await this.redis.get(lockKey);
            if (value !== null) {
                console.log("Lock attempt " + attempt + " of " + maxAttempts + " failed because locked by: " + value);
                await sleep(waitingSeconds);
            }
        } while (value !== null);

        // Then we set the lock using a TTL of 60 seconds, every 30 seconds.
        const setHeartbeat = async () => {
            await this.redis.setex(lockKey, 60, this.instanceId);
        }
        this.lockingHeartbeat = setInterval(() => {
            setHeartbeat();
        }, 30 * 1000);
        await setHeartbeat();

        await sleep(5);

        // Make 1 attempt that just double checks nothing overwrote the lock in the mean time
        const currentLockValue = await this.redis.get(lockKey);
        if (currentLockValue !== this.instanceId) {
            console.error("Current lock value is wrong, expecting " + this.instanceId + " but stolen by " + currentLockValue);
            process.exit(1);
        }

        setInterval(async () => {
            await this.checkCycle();
        }, 5 * 1000);

        await this.checkCycle();
        
        

    }

    async checkCycle() : Promise<boolean> {
        if (this.isPaused) {
            return;
        }

        if (this.isProcessing) {
            return;
        }
        this.isProcessing = true;

        try {
            // TODO
            // Continuously attempt to get the next message and process it by writing the values to the target
            // When waiting for more events, pause for max X seconds and also create a LISTENER that auto triggers when something happens
            // If connection goes down, all bets are off.  A cycle is triggered upon reconnection (ready event)


            this.isProcessing = false;
        } catch(e: any) {
            console.error(e);
            console.error("Pausing for 30 seconds until next check cycle");
            await sleep(30);
            this.isProcessing = false; // Unlock for next interval
        }
    }
}