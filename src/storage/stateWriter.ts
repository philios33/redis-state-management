import { Redis } from "ioredis";
import { ReliableRedisClient } from "../reliableRedisClient";
import { RedisQueuesController } from "../queues/controller";


export class RedisStorageStateWriter {
    redis: Redis;
    namespace: string;
    queues: RedisQueuesController;
    incomingQueueId: string;

    constructor(rrc: ReliableRedisClient, namespace: string, incomingQueueId: string) {
        this.redis = rrc.getClient();
        this.namespace = namespace;

        this.queues = new RedisQueuesController(rrc, namespace);
        this.incomingQueueId = incomingQueueId;
    }

    private async _pushStore(type: string, meta: any) {
        const message = {
            type,
            occurredAt: (new Date()).toISOString(),
            meta
        }
        await this.queues.pushMessage(this.incomingQueueId, message);
    }

    async writeStateObj<T>(id: string, value: T): Promise<void> {
        await this._pushStore("WRITE_STATE_OBJECT", {
            id,
            value
        });
    }

    async removeStateObj(id: string): Promise<void> {
        throw new Error("Use writeStateObj and {} to reset a state variable");
    }

    // Normal string variable
    async setValue(id: string, value: any): Promise<void> {
        await this._pushStore("WRITE_SIMPLE_VALUE", {
            id,
            value
        });
    }

    async delValue(id: string): Promise<void> {
        throw new Error("Please use setValue with a null value to delete");
    }

    async setHashmapValue<T>(id: string, key: string, value: T | null) : Promise<void> {
        await this._pushStore("WRITE_HASHMAP_VALUE", {
            id,
            key,
            value
        });
    }

    async delHashmapValue(id: string, key: string) : Promise<void> {
        await this.setHashmapValue(id, key, null);
        // throw new Error("Please use setHashmapValue with null value to delete");
    }

    async addToStringSet(id: string, values: Array<string>): Promise<void> {
        await this._pushStore("ADD_STRINGS_TO_SET", {
            id,
            values,
        });
    }
    async removeFromStringSet(id: string, values: Array<string>): Promise<void> {
        await this._pushStore("REMOVE_STRINGS_FROM_SET", {
            id,
            values,
        });
    }

}