
// This thread processes messages from the incoming queue and applies them to the correct variables
// Note: The functionality in this class is NOT thread safe, but is designed to work as a singleton instance.
// A synchronisation lock variable is checked during start function to prevent more than one instance of this code processing the queue.

import { Redis } from "ioredis";
import { compare } from "deep-diff-patcher";
import { v4 as uuidv4 } from "uuid";

import { RedisQueuesController } from "../queues/controller";
import { ReliableRedisClient } from "../reliableRedisClient";
import { DiffMessage, GenericMessageWithId, StateVersion } from "../types";
import { serialize, unserialize } from "./serializer";

export class RedisStorageProcessor {
    rrc: ReliableRedisClient;
    redis: Redis | null;
    namespace: string;
    queues: RedisQueuesController;
    incomingQueueId: string;
    instanceId: string;
    isPaused: boolean;
    isStarted: boolean;
    isStopping: boolean;
    isStopped: boolean;
    lockingHeartbeat: ReturnType<typeof setInterval> | null;
    waitingUntil: Date | null;

    constructor(rrc: ReliableRedisClient, namespace: string, incomingQueueId: string) {
        this.rrc = rrc;
        this.redis = null;
        this.namespace = namespace;

        this.queues = new RedisQueuesController(rrc, namespace);
        this.incomingQueueId = incomingQueueId;
        this.instanceId = uuidv4();
        this.isPaused = false;
        this.isStarted = false;
        this.isStopping = false;
        this.isStopped = false;
        this.lockingHeartbeat = null;
        this.waitingUntil = null;
        
    }

    private _unserialize<T>(str: string) : T {
        return unserialize(str);
    }

    private _serialize<T>(v: T) : string {
        return serialize(v);
    }

    private _calculateDiff(obj1: any, obj2: any) : any {
        return compare(obj1, obj2);
    }

    async sleep(ms: number) {
        return new Promise((resolve, reject) => {
            setTimeout(() => {
                resolve(null);
            }, ms);
        });
    }

    async start() {
        if (this.isStarted) {
            throw new Error("Cannot start twice");
        }
        this.isStarted = true;

        this.redis = await this.rrc.getClient(); // Hangs until connected

        // We attempt a crude lock mechanism by waiting for a variable to be null in redis
        const lockKey = "STORAGE_PROCESSOR_" + this.namespace;
        const maxAttempts = 10;
        const waitingSeconds = 10;
        let attempt = 0;
        let value;
        do {
            attempt++;
            if (attempt > maxAttempts) {
                throw new Error("Maximum locking attempts exceeded: " + maxAttempts);
            }
            value = await this.redis.get(lockKey);
            if (value !== null) {
                console.log("Lock attempt " + attempt + " of " + maxAttempts + " failed because locked by: " + value);
                await this.sleep(waitingSeconds * 1000);
            }
        } while (value !== null);

        // Then we set the lock using a TTL of 60 seconds, every 30 seconds.
        const setHeartbeat = async () => {
            if (this.redis !== null) {
                await this.redis.setex(lockKey, 60, this.instanceId);
            }
        }
        
        await setHeartbeat();

        await this.sleep(5000);

        // Make 1 attempt that just double checks nothing overwrote the lock in the mean time
        const currentLockValue = await this.redis.get(lockKey);
        if (currentLockValue !== this.instanceId) {
            throw new Error("Current lock value is wrong, expecting " + this.instanceId + " but stolen by " + currentLockValue);
        }

        this.lockingHeartbeat = setInterval(() => {
            setHeartbeat();
        }, 30 * 1000);

        // We have a single execution loop that just repeats always but hangs a bit when it cannot proceed
        // Other events can triggerWaitingCycle to attempt to unclog the main loop, but it will only succeed if it is waiting
        this.continuouslyCheckCycle(); // Note: Don't await so that the start function returns

        this.redis.on("ready", () => {
            // This is for handling a reconnection that has caused the loop to stop
            this.triggerWaitingCycle();
        });
    }

    async stop() {
        if (this.lockingHeartbeat !== null) {
            clearInterval(this.lockingHeartbeat);
            this.lockingHeartbeat = null;
        }
        this.isStopping = true;
        while (!this.isStopped) {
            await this.sleep(1000);
        }
    }

    async continuouslyCheckCycle(): Promise<void> {
        while (!this.isStopping) {
            await this.checkCycle();
        }
        this.isStopped = true;
    }

    pause() {
        this.isPaused = true;
    }
    unPause() {
        this.isPaused = false;
        this.triggerWaitingCycle();
    }

    triggerWaitingCycle() {
        this.waitingUntil = null;
    }

    // This function is repeatedly run forever
    async checkCycle() : Promise<void> {
        
        try {
            if (this.isPaused) {
                throw new Error("Is paused");
            }
    
            if (!this.rrc.isConnected) {
                throw new Error("Not connected to redis yet");
            }
        
            // Continuously attempt to get the next message and process it by writing the values to the target
            let isMoreMessages = true;
            while (isMoreMessages) {
                if (this.isStopping) {
                    return;
                }

                const nextMessage = await this.queues.popNextMessage(this.incomingQueueId);
                if (nextMessage === null) {
                    // console.log("No more messages...");
                    isMoreMessages = false;
                } else {
                    // console.log("Handling message " + nextMessage.id);
                    await this.processRedisStorageMessage(nextMessage);
                }
            }

            // Queue now looks empty
            // Then when waiting for more events, create a LISTENER that auto triggers when something happens
            // Note, when you SUBSCRIBE, the connection goes in to a waiting state so will not function properly to read the queue until it is unlocked by ending the subscription properly
            // This is all handled by the queues controller
            const control = {
                isCancelled: false
            }
            const cancelTimeout = setTimeout(() => {
                control.isCancelled = true;
            }, 300 * 1000); // After 5 minutes

            // Or check every second for this.isStopping
            const cancelInterval = setInterval(() => {
                if (this.isStopping) {
                    control.isCancelled = true;
                }
            }, 1000);

            try {
                // console.log("WAITING FOR MORE MESSAGES TO HANDLE");
                await this.queues.hangConnectionUntilNextMessageOrCancelled(this.incomingQueueId, control);
                // console.log("GOT A SIGNAL THAT THERE ARE MORE");
            } catch(e: any) {
                // console.log("WAITING THREW AN ERROR: " + e.message);
                // We catch this error on timeout or connection error, because we can handle both things by just repeating the loop anyway
                console.warn(e);
            }
            clearTimeout(cancelTimeout);
            clearInterval(cancelInterval);
            
            // If connection goes down, all bets are off.  An error should be thrown and the loop will get in to a waiting state (see below)
            // Then a reconnection (ready event) will trigger the waiting loop to immediately end by nulling the waitingUntil value which causes the function to return and repeat the loop
        } catch(e: any) {
            console.error(e);
            console.error("Waiting to be retriggered for 300 seconds");
            this.waitingUntil = new Date();
            this.waitingUntil.setSeconds(this.waitingUntil.getSeconds() + 300);

            while (this.waitingUntil !== null && new Date() < this.waitingUntil) {
                await this.sleep(500);
            }
            this.waitingUntil = null;
        }
    }

    async processRedisStorageMessage(message: GenericMessageWithId) {
        if (this.redis === null) {
            throw new Error("Not started redis yet");
        }

        // There is often no need to unserialize the value from the event just to reserialize back to redis
        if (message.message.type === "WRITE_SIMPLE_VALUE") {
            const key = this.namespace + "-VAL-" + message.message.meta.key;
            const value = message.message.meta.value;
            await this.redis.set(key, value);

        } else if (message.message.type === "WRITE_STATE_OBJECT") {
            const key = this.namespace + "-STATE-" + message.message.meta.key;
            const value = this._unserialize(message.message.meta.value);
            const deltaUpdatesChannelId = this.namespace + "-STATE-" + message.message.meta.key + "-DELTA";

            // But here we DO need to unserialize to read the counter
            const currentStr = await this.redis.get(key);
            let oldState: any = {};
            let nextVersion = 1;
            if (currentStr !== null) {
                const current = this._unserialize(currentStr) as StateVersion<any>;
                // Expect a state version here
                nextVersion = current.version + 1;
                oldState = current.value;
            }

            // Write next value
            const now = (new Date()).toISOString();
            if (JSON.stringify(value) == "{}") { 
                // Special value meaning delete the variable
                await this.redis.del(key);
            } else {
                const newStateVersion: StateVersion<any> = {
                    version: nextVersion,
                    value: value,
                    writtenAt: now,
                }
                await this.redis.set(key, this._serialize(newStateVersion));
            }

            // Publish diff
            const deltaPayload = this._calculateDiff(oldState, value);
            const diffMsg: DiffMessage = {
                fromVersion: nextVersion - 1,
                toVersion: nextVersion,
                writtenAt: now,
                deltaPayload: deltaPayload,
            }
            await this.redis.publish(deltaUpdatesChannelId, this._serialize(diffMsg));

        } else if (message.message.type === "WRITE_HASHMAP_VALUE") {
            const key = this.namespace + "-MAP-" + message.message.meta.key;
            const field = message.message.meta.field;
            const value = message.message.meta.value;
            await this.redis.hmset(key, field, value);

        } else if (message.message.type === "ADD_STRINGS_TO_SET") {
            const key = this.namespace + "-SET-" + message.message.meta.key;
            const values = message.message.meta.values;
            await this.redis.sadd(key, values);

        } else if (message.message.type === "REMOVE_STRINGS_FROM_SET") {
            const key = this.namespace + "-SET-" + message.message.meta.key;
            const values = message.message.meta.values;
            await this.redis.srem(key, values);

        } else {
            console.warn("Cannot process message type: " + message.message.type);
        }
        
        // then confirm
        await this.queues.confirmMessageById(this.incomingQueueId, message.id);
    }
}