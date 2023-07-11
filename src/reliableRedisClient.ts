// Wraps redis client functionality by setting up a auto reconnecting redis client with warnings reporting to console
// Centralises reconnection strategy and gives better debugging and event handling
// Note: ioredis is leaps and bounds better than node-redis when it comes to handling unexpected disconnections
import Redis from "ioredis";

type ConnectingEvent = {
    type: "CONNECTING"
    attempt: number
}

type ReconnectingEvent = {
    type: "RECONNECTING"
    attempt: number
    reconnectingForSecs: number
}

type FirstReadyEvent = {
    type: "FIRST_READY"
}

type ReconnectedEvent = {
    type: "RECONNECTED"
    wasDisconnectedForSecs: number
}

type ConnectionEvent = ConnectingEvent | ReconnectingEvent | FirstReadyEvent | ReconnectedEvent;

export class ReliableRedisClient {

    private connectionName: string;
    private redisHost: string;
    private redisPort: number;

    private client: null | Redis;

    private isStarting: boolean
    private isStarted: boolean
    private isStopped: boolean
    
    private connectionAttempt: number
    private lastConnectedAt: null | Date;
    private lastDisconnectAt: null | Date;

    private disconnectWarning: boolean;

    constructor(connectionName: string, redisHost: string, redisPort: number) {

        this.connectionName = connectionName;
        this.redisHost = redisHost;
        this.redisPort = redisPort;

        this.client = null;
        this.isStarting = false;
        this.isStarted = false;
        this.isStopped = false;

        this.connectionAttempt = 0;
        this.lastConnectedAt = null;
        this.lastDisconnectAt = null;
        this.disconnectWarning = false; // This keeps track of whether a 15 second disconnect debug message has been sent

    }

    private getNewRedisClient() {
        const client = new Redis({
            host: this.redisHost,
            port: this.redisPort,

            autoResendUnfulfilledCommands: false, // If a command is unconfirmed and timesout then its a failure of that command.

            autoResubscribe: false, // Never resubscribe any subscriptions automatically.  Implementations may need to handle the subscription creation directly.
            enableOfflineQueue: false, // Never keep an offline queue of commands.  Commands should fail, cause an error, be handled and retry at appropriate times if the connection is offline.
            commandQueue: false,
            lazyConnect: true, // This allows us to wait for connection ourselves since we dont use the offline queue

            // Note: Retrying forever sounds like the most recoverable option but is not very useful in reality.  
            // It can cause processing processes to remain alive but in a hung state forever.
            // It is better for commands to fail and processes to die so potential issues don't go unnoticed.
            // Note: This is the maximum connection retries per command
            maxRetriesPerRequest: 10,

            // Note: This is the retry connection strategy
            retryStrategy(times) {
                const delay = Math.min(times * 500, 5000);
                return delay;
            },
        });
        return client;
    }

    async start(eventsCallback?: ((event: ConnectionEvent) => void)) {
        if (this.isStopped) {
            throw new Error("Is already stopped, cannot start");
        }
        if (this.isStarting) {
            throw new Error("Already started up by something else");
            // return false; // Already started up by something else
        }
        this.isStarting = true;

        this.client = this.getNewRedisClient();
        this.client.on('error', (e: any) => {
            if (e.code === "ECONNREFUSED") {
                // Ignore
            } else {
                console.warn("[Redis: " + this.connectionName + "] Error - " + e);
            }
        });
        this.client.on('connect', () => {
            // console.log("REDIS CLIENT " + this.connectionId + ": Connected");
        });
        this.client.on('ready', () => {
            const now = new Date();

            if (this.lastDisconnectAt !== null) {
                const duration = Math.round((now.getTime() - this.lastDisconnectAt.getTime()) / 1000);
                console.log("[Redis: " + this.connectionName + "] Re-connected to Redis after being down for " + duration + " secs");
                if (eventsCallback) {
                    eventsCallback({
                        type: "RECONNECTED",
                        wasDisconnectedForSecs: duration,
                    });
                }
            } else {
                console.log("[Redis: " + this.connectionName + "] Connected to Redis!");
                if (eventsCallback) {
                    eventsCallback({
                        type: "FIRST_READY",
                    });
                }
            }

            this.connectionAttempt = 0;
            this.disconnectWarning = false;
            this.lastDisconnectAt = null;
            this.lastConnectedAt = new Date();
        });
        this.client.on('close', () => {
            // Not an error, just the connection closed
            // console.error("REDIS CLIENT " + this.connectionId + ": Close");
        });
        this.client.on('connecting', () => {
            if (this.lastConnectedAt === null) {
                this.connectionAttempt++;
                if (eventsCallback) {
                    eventsCallback({
                        type: "CONNECTING",
                        attempt: this.connectionAttempt,
                    });
                }
            }
        });
        this.client.on('reconnecting', () => {

            if (this.lastConnectedAt === null) {
                return; // Never connected yet, ignore this one since connect event still fires properly
            }
            this.connectionAttempt++;
            if (this.lastDisconnectAt === null) {
                this.lastDisconnectAt = new Date();
                console.warn("[Redis: " + this.connectionName + "] Warning, we are disconnected from redis!");
            }
            const now = new Date();
            const timeReconnecting = Math.round((now.getTime() - this.lastDisconnectAt.getTime()) / 1000);
            if (timeReconnecting > 15) {
                if (!this.disconnectWarning) {
                    this.disconnectWarning = true;
                    console.warn("[Redis: " + this.connectionName + "] Warning, been disconnected from redis for " + timeReconnecting + " secs..");
                }
            }

            if (eventsCallback) {
                eventsCallback({
                    type: "RECONNECTING",
                    attempt: this.connectionAttempt,
                    reconnectingForSecs: timeReconnecting,
                });
            }
        });

        const timeoutMs = 30 * 1000;
        try {
            await this.client.connect();
        } catch(e: any) {
            // First connection attempt failed, but it will retry
            console.warn("[Redis: " + this.connectionName + "] Warning, the first connection attempt to Redis " + this.redisHost + ":" + this.redisPort + " failed, but will continue to retry for " + timeoutMs + " ms...");
        }

        let hasTimedOut = false;
        const connectionTimeout = setTimeout(() => {
            hasTimedOut = true;
        }, timeoutMs);
        while (!hasTimedOut && (this.lastDisconnectAt !== null || this.lastConnectedAt === null)) {
            // Not connected yet
            // console.warn("Still no connected yet");
            await this.sleep(1000);
        }
        if (hasTimedOut) {
            this.client.disconnect(false);
            throw new Error("Connection timeout occurred after: " + timeoutMs);
        }
        clearTimeout(connectionTimeout);

        this.isStarted = true;
    }

    sleep(ms: number) {
        return new Promise((resolve, reject) => {
            setTimeout(() => {
                resolve(null);
            }, ms);
        })
    }

    getClient() : Redis {
        if (this.client === null) {
            throw new Error("You must startup this controller");
        } else {
            if (this.isStarted) {
                return this.client;
            } else {
                throw new Error("Redis client still starting up, please await the startup function properly");
            }
        }
    }

    stop() {
        if (this.isStopped) {
            throw new Error("Is already stopped");
        }
        if (!this.isStarted) {
            throw new Error("Not started up properly yet, please await properly");
        }
        const client = this.getClient();
        client.removeAllListeners();
        client.disconnect(false);
        this.isStopped = true;
    }
}
