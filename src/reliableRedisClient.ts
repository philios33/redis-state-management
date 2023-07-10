// Wraps redis client functionality by setting up a auto reconnecting redis client with warnings reporting to console
// Centralises reconnection strategy and gives better debugging
// Note: ioredis is leaps and bounds better than node-redis when it comes to handling unexpected disconnections
import Redis from "ioredis";

export class ReliableRedisClient {

    client: Redis;
    connectionName: string;
    lastConnectedAt: null | Date;
    lastDisconnectAt: null | Date;
    disconnectWarning: boolean;

    constructor(connectionName: string, redisHost: string, redisPort: number) {

        this.connectionName = connectionName;
        this.lastConnectedAt = null;
        this.lastDisconnectAt = null;
        this.disconnectWarning = false;


        this.client = new Redis({
            port: redisPort,
            host: redisHost,

            autoResendUnfulfilledCommands: false, // If a command is unconfirmed and timesout then its a failure of that command.

            autoResubscribe: false, // Never resubscribe any subscriptions automatically.  Implementations may need to handle the subscription creation directly.
            enableOfflineQueue: false, // Never keep an offline queue of commands.  Commands should fail, cause an error, be handled and retry at appropriate times if the connection is offline.

            // Note: Retrying forever sounds like the most recoverable option but is not very useful in reality.  
            // It can cause processing processes to remain alive but in a hung state forever.
            // It is better for commands to fail and processes to die so potential issues don't go unnoticed.
            // Note: This is the maximum connection retries per command
            maxRetriesPerRequest: 10,

            // Note: This is the retry connection strategy
            retryStrategy(times) {
                // Always 2 second delay, so that commands will take a maxium of 20 seconds (10 * 2) until they timeout.
                return 2000;
            },

            lazyConnect: true, // This allows us to wait for connection ourselves since we dont use the offline queue
        });

        this.client.on("reconnecting", (ms: number) => {
            if (this.lastDisconnectAt === null) {
                this.lastDisconnectAt = new Date();
                console.warn("[Redis: " + this.connectionName + "] Warning, we are disconnected from redis!");
            }
            const now = new Date();
            const timeReconnecting = now.getTime() - this.lastDisconnectAt.getTime();
            if (timeReconnecting > 15 * 1000) {
                if (!this.disconnectWarning) {
                    this.disconnectWarning = true;
                    console.warn("[Redis: " + this.connectionName + "] Warning, been disconnected from redis for " + timeReconnecting + " ms...");
                }
            }
        });

        this.client.on("ready", () => {

            if (this.lastDisconnectAt !== null) {
                const duration = Math.round(((new Date()).getTime() - this.lastDisconnectAt.getTime()) / 1000);
                console.log("[Redis: " + this.connectionName + "] Re-connected to Redis after being down for " + duration + " secs");
            } else {
                console.log("[Redis: " + this.connectionName + "] Connected to Redis!");
            }

            this.disconnectWarning = false;
            this.lastDisconnectAt = null;
            this.lastConnectedAt = new Date();
        });
        
        this.client.on("error", (e: any) => {
            if (e.code === "ECONNREFUSED") {
                // Ignore
            } else {
                console.warn("[Redis: " + this.connectionName + "] Error - " + e);
            }
        });
    }

    async connect() : Promise<void> {
        // This MUST be called once to ensure that we are connected the first time
        await this.client.connect();
    }

    isConnected() : boolean {
        return this.lastConnectedAt !== null && this.lastDisconnectAt === null;
    }

    async getClient() : Promise<Redis> {
        // Since we don't have an offline queue enabled, we have to wait for the ready event here
        // await this.client.connect(); // Or just harness the logic in client.connect like this
        // Now we can just always fast return the client since we are always considered connected
        return this.client;

        /*
        const timeout = 10 * 1000;
        const expiry = new Date().getTime() + timeout;
        return new Promise((resolve, reject) => {
            let connected = false;

            const finish = (error: any, result?: any) => {
                clearInterval(interval);
                if (error) {
                    reject(error);
                } else {
                    resolve(result);
                }
            }
            const interval = setInterval(() => {
                if (this.isConnected()) {
                    finish(null, this.client);
                }
                if (new Date().getTime() > expiry) {
                    finish(new Error("Timedout connecting"));
                }
            }, 500);
        });
        */
    }

    shutdown() {
        this.client.removeAllListeners();
        this.client.disconnect(false);
    }
}
