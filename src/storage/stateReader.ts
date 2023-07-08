import { Redis } from "ioredis";
import { ReliableRedisClient } from "../reliableRedisClient";
import { StateVersion } from "../types";


export class RedisStorageStateReader {
    redis: Redis;
    namespace: string;

    constructor(rrc: ReliableRedisClient, namespace: string) {
        this.redis = rrc.getClient();
        this.namespace = namespace;
    }

    private _unserialize<T>(str: string) : T {
        return JSON.parse(str);
    }


    async readStateObj<T>(id: string): Promise<StateVersion<T> | null> {
        const stateVarId = this.namespace + "-STATE-" + id;
        const currentStr = await this.redis.get(stateVarId);
        if (currentStr !== null) {
            // Variable exists
            const current = this._unserialize(currentStr) as StateVersion<T>;
            return current;
        } else {
            return null;
        }
    }

    async getValue(id: string): Promise<any> {
        const value = await this.redis.get(this.namespace + "-VAL-" + id);
        if (value === null) {
            return null;
        } else {
            return this._unserialize(value);
        }
    }

    async getHashmapValue<T>(id: string, key: string) : Promise<T | null> {
        const mapId = this.namespace + "-MAP-" + id;
        const value = await this.redis.hget(mapId, key);
        if (value === null) {
            return null;
        } else {
            return this._unserialize(value);
        }
    }

    async getAllHashmapValues<T>(id: string) : Promise<Array<T>> {
        const mapId = this.namespace + "-MAP-" + id;
        const values = await this.redis.hvals(mapId);
        return values.map(v => this._unserialize(v));
    }

    async getHashmapAsRecord<T>(id: string) : Promise<Record<string, T>> {
        const mapId = this.namespace + "-MAP-" + id;
        const stringValues = await this.redis.hgetall(mapId);
        const objValues: Record<string, T> = {};
        for (const key in stringValues) {
            objValues[key] = this._unserialize<T>(stringValues[key]);
        }
        return objValues;
    }

    async getHashmapSize(id: string) : Promise<number> {
        const mapId = this.namespace + "-MAP-" + id;
        return await this.redis.hlen(mapId);
    }

    async getStringSet(id: string): Promise<string[]> {
        return await this.redis.smembers(this.namespace + "-SET-" + id);
    }



}