import { ReliableRedisClient } from "./reliableRedisClient";
import { RedisStorageProcessor } from "./storage/processor";

const rrc = new ReliableRedisClient("TEST", "localhost", 6379);

(async () => {
    try {
        await rrc.start();
        const proc = new RedisStorageProcessor(rrc, "TEST", "in");
        await proc.start();
        console.log("Started processor...");
    } catch(e: any) {
        console.error(e);
    }
})();

