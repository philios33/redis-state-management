// Wraps redis client functionality by setting up a auto reconnecting redis client with warnings reporting to console
// Centralises reconnection strategy and gives better debugging
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

            // Note: Retrying forever sounds like the most recoverable option but is not very useful in reality.  
            // It can cause processing processes to remain alive but in a hung state forever.
            // It is better for commands to fail and processes to die so potential issues don't go unnoticed.
            maxRetriesPerRequest: 30,

            retryStrategy(times) {
                // Always 2 second delay, so we retry 30 times for a 60 second timeout!
                return 2000;
            },
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

    getClient() : Redis {
        return this.client;
    }
}
