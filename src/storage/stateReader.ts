import { Redis } from "ioredis";
import { ReliableRedisClient } from "../reliableRedisClient";
import { DiffMessage, StateVersion } from "../types";
import { unserialize } from "./serializer";


export class RedisStorageStateReader {
    rrc: ReliableRedisClient;
    namespace: string;

    constructor(rrc: ReliableRedisClient, namespace: string) {
        this.rrc = rrc;
        this.namespace = namespace;
    }

    private _unserialize<T>(str: string) : T {
        return unserialize(str);
    }


    async readStateObj<T>(id: string): Promise<StateVersion<T> | null> {
        const redis = await this.rrc.getClient();
        const stateVarId = this.namespace + "-STATE-" + id;
        const currentStr = await redis.get(stateVarId);
        if (currentStr !== null) {
            // Variable exists
            const current = this._unserialize(currentStr) as StateVersion<T>;
            return current;
        } else {
            return null;
        }
    }

    async getValue(id: string): Promise<any> {
        const redis = await this.rrc.getClient();
        const value = await redis.get(this.namespace + "-VAL-" + id);
        if (value === null) {
            return null;
        } else {
            return this._unserialize(value);
        }
    }

    async getHashmapValue<T>(id: string, key: string) : Promise<T | null> {
        const mapId = this.namespace + "-MAP-" + id;
        const redis = await this.rrc.getClient();
        const value = await redis.hget(mapId, key);
        if (value === null) {
            return null;
        } else {
            return this._unserialize(value);
        }
    }

    async getAllHashmapValues<T>(id: string) : Promise<Array<T>> {
        const mapId = this.namespace + "-MAP-" + id;
        const redis = await this.rrc.getClient();
        const values = await redis.hvals(mapId);
        return values.map(v => this._unserialize(v));
    }

    async getHashmapAsRecord<T>(id: string) : Promise<Record<string, T>> {
        const mapId = this.namespace + "-MAP-" + id;
        const redis = await this.rrc.getClient();
        const stringValues = await redis.hgetall(mapId);
        const objValues: Record<string, T> = {};
        for (const key in stringValues) {
            objValues[key] = this._unserialize<T>(stringValues[key]);
        }
        return objValues;
    }

    async getHashmapSize(id: string) : Promise<number> {
        const mapId = this.namespace + "-MAP-" + id;
        const redis = await this.rrc.getClient();
        return await redis.hlen(mapId);
    }

    async getStringSet(id: string): Promise<string[]> {
        const redis = await this.rrc.getClient();
        return await redis.smembers(this.namespace + "-SET-" + id);
    }

    // TODO No LIST functionality yet, perhaps we will need that

    async fetchStateAndListen<T>(key: string, fullCallback: (full: StateVersion<T>) => void, deltaCallback: (delta: DiffMessage) => void, errorCallback: (error: any) => void): Promise<() => void> {
        // console.log("Duplicating new client");
        const client = this.rrc.getClient().duplicate();
        try {
            await client.connect();
        } catch(e: any) {
            // First connect failed, cleanup the duplicate client
            client.disconnect(false);
            throw e;
        }

        // Connected here
        const cleanupClient = () => {
            // console.log("Cleaning up client");
            client.removeAllListeners();
            client.disconnect(false);
        }
        const handleError = (error: any) => {
            try {
                errorCallback(error);
            } catch(e: any) {
                console.error("Error in error callback", e);
            }
        }

        try {
            let currentVersion: null | number = null;

            const deltaUpdatesChannelId = this.namespace + "-STATE-" + key + "-DELTA";
            client.on("message", (channel, message) => {
                try {
                    const diff = this._unserialize<DiffMessage>(message);
                    if (diff.fromVersion === currentVersion) {
                        currentVersion = diff.toVersion;
                        deltaCallback(diff);
                    } else {
                        console.warn("Ignoring delta from version: " + diff.fromVersion + " since we have current version " + currentVersion);
                    }
                } catch(e: any) {
                    cleanupClient();
                    handleError(e);
                }
            });

            const initialise = async () => {
                // console.log("Subscribing client");
                await client.subscribe(deltaUpdatesChannelId);
                const current = await this.readStateObj<T>(key);
                if (current === null) {
                    throw new Error("Missing state object: " + key);
                }
                currentVersion = current.version;
                fullCallback(current);
            }
            await initialise();
            let isInitialised = true;

            // Setup a reconnection handler here
            client.on("ready", () => {
                // This event fires when the client gets reconnected.
                if (!isInitialised) {
                    initialise().then(() => {
                        isInitialised = true;
                    });
                }
            });
            client.on("reconnecting", () => {
                isInitialised = false;
            })
            
        } catch(e: any) {
            cleanupClient();
            throw e;
        }

        return () => {
            handleError(new Error("Unsubscribed"));
            cleanupClient();
        }
    }
}