
import Redis from "ioredis";
import { ReliableRedisClient } from "../reliableRedisClient";
import { GenericMessage, GenericMessageWithId } from "../types";

export class RedisQueuesController {

    rrc: ReliableRedisClient;
    namespace: string;

    constructor(rrc: ReliableRedisClient, namespace: string) {
        this.rrc = rrc;
        this.namespace = namespace;
    }

    // Note: We need to prevent message loss when connection to redis goes down, or if handler crashes.
    // If a message was not handled properly, it should be put back on the end of the queue for reprocessing later.
    // By moving a single message to a staging variable (AKA processing list) during processing, we can ensure that we don't miss a message completely.
    // Due to how messages are handled, this could cause multiple half handling of the same message.  (At least once)
    // Thus, we should strive for idempotency when building the system.
    // There are many processors, so if one crashes it is not the end of the world, but if we miss a message it could cause unexpected side effects throughout the calculated state.

    // See reliable queue pattern: https://redis.io/commands/lmove/

    async pushMessage(queueId: string, message: GenericMessage): Promise<number> {
        // LPUSH queue xxx
        const redis = await this.rrc.getClient();
        const value = await redis.lpush(this.namespace + "-Q-" + queueId, this._serialize(message));

        // PUB/SUB logic for triggering queues
        const channelId = this.namespace + "-Q-" + queueId + "-CHANNEL";
        await redis.publish(channelId, "PUSH");

        return value;
    }

    async popNextMessage(queueId: string): Promise<GenericMessageWithId | null> {
        const redis = await this.rrc.getClient();
        // OLD Method
        /*
        // RPOP queue
        const next = await this.redis.rpop("Q-" + queueId);
        if (next === null) {
            return null;
        } else {
            return this._unserialize(next);
        }
        */

        const queueListName = this.namespace + "-Q-" + queueId;
        const processingListName = this.namespace + "-QP-" + queueId;

        // Before we try to pop the next message, check the processing list is empty, otherwise push back on to the end of the queue
        let processingLength = await redis.llen(processingListName);
        if (processingLength > 0) {
            do {
                console.warn("There are " + processingLength + " unprocessed messages in queue " + queueId + ", re-pushing them now...");
                await redis.lmove(processingListName, queueListName, "LEFT", "RIGHT");

                processingLength = await redis.llen(processingListName);
            } while(processingLength > 0);

        }

        const next = await redis.lmove(queueListName, processingListName, "RIGHT", "LEFT");
        if (next === null) {
            return null;
        } else {
            return {
                id: next,
                message: this._unserialize(next)
            };
        }
    }

    async confirmMessageById(queueId: string, messageId: string) {
        const redis = await this.rrc.getClient();
        const removed = await redis.lrem(this.namespace + "-QP-" + queueId, 1, messageId);
        if (removed !== 1) {
            console.error("Could not confirm message: " + messageId);
            console.error("A processor tried to remove a message from the processsing queue that is not there.");
            console.error("This suggests data inconsistancy between redis and a processor, or a race condition between multiple processors.");
            throw new Error("Could not confirm message: " + messageId);
        }
    }

    async hangConnectionUntilNextMessageOrCancelled(queueId: string, control: {isCancelled: boolean}) {
        const redis = await this.rrc.getClient();
        const client = redis.duplicate();
        await client.connect();

        return new Promise((resolve, reject) => {

            const cancelledInterval = setInterval(() => {
                if (control.isCancelled) {
                    finish(new Error("Listening cancelled"));
                }
            }, 1000);

            let isFinished = false;
            const finish = (error: any, result?: any) => {
                if (!isFinished) {
                    isFinished = true;

                    clearInterval(cancelledInterval);

                    // No need to unsubscribe, just quit and throw away this client
                    client.quit(() => {
                        if (error) {
                            reject(error);
                        } else {
                            resolve(result);
                        }
                    });
                }
            }

            try {
                client.on("message", (channel, message) => {
                    finish(null, message);
                });
                client.subscribe(this.namespace + "-Q-" + queueId + "-CHANNEL", (error) => {
                    if (error) {
                        finish(error);
                    }
                });

            } catch(e) {
                reject(e);
            }
        })
    }

    async deleteQueue(queueId: string) {
        const queueListName = this.namespace + "-Q-" + queueId;
        const processingListName = this.namespace + "-QP-" + queueId;

        const redis = await this.rrc.getClient();
        await redis.del(queueListName);
        await redis.del(processingListName);
    }

    async getQueueSize(queueId: string): Promise<number> {
        // LLEN queue
        const redis = await this.rrc.getClient();
        return await redis.llen(this.namespace + "-Q-" + queueId);
    }

    private _serialize(message: GenericMessage) : string {
        // TODO, use lib that support date objects properly
        return JSON.stringify(message);
    }

    private _unserialize(str: string) : GenericMessage {
        return JSON.parse(str);
    }
}