
import { ReliableRedisClient } from "../reliableRedisClient";
import { RedisQueuesController } from "../queues/controller";
import { serialize, unserialize } from "./serializer";


export class RedisStorageStateWriter {
    namespace: string;
    queues: RedisQueuesController;
    incomingQueueId: string;

    constructor(rrc: ReliableRedisClient, namespace: string, incomingQueueId: string) {
        this.namespace = namespace;

        this.queues = new RedisQueuesController(rrc, namespace);
        this.incomingQueueId = incomingQueueId;
    }

    private _unserialize<T>(str: string) : T {
        return unserialize(str);
    }

    private _serialize<T>(v: T) : string {
        return serialize(v);
    }

    private async _pushStore(type: string, meta: any) {
        const message = {
            type,
            occurredAt: (new Date()).toISOString(),
            meta
        }
        await this.queues.pushMessage(this.incomingQueueId, message);
    }

    async writeStateObj<T>(key: string, value: T): Promise<void> {
        await this._pushStore("WRITE_STATE_OBJECT", {
            key,
            value: this._serialize(value),
        });
    }

    async removeStateObj(key: string): Promise<void> {
        await this.writeStateObj(key, {});
        // throw new Error("Use writeStateObj and {} to reset a state variable");
    }

    // Normal string variable
    async setValue(key: string, value: any): Promise<void> {
        await this._pushStore("WRITE_SIMPLE_VALUE", {
            key,
            value: this._serialize(value),
        });
    }

    async delValue(key: string): Promise<void> {
        throw new Error("Please use setValue with a null value to delete");
    }

    async setHashmapValue<T>(key: string, field: string, value: T | null) : Promise<void> {
        await this._pushStore("WRITE_HASHMAP_VALUE", {
            key,
            field,
            value: this._serialize(value),
        });
    }

    async delHashmapValue(key: string, field: string) : Promise<void> {
        await this.setHashmapValue(key, field, null);
        // throw new Error("Please use setHashmapValue with null value to delete");
    }

    async addToStringSet(key: string, values: Array<string>): Promise<void> {
        await this._pushStore("ADD_STRINGS_TO_SET", {
            key,
            values,
        });
    }
    async removeFromStringSet(key: string, values: Array<string>): Promise<void> {
        await this._pushStore("REMOVE_STRINGS_FROM_SET", {
            key,
            values,
        });
    }

    // TODO No LIST functionality yet, perhaps we will need that
}