import { ReliableRedisClient } from "./reliableRedisClient";
import { RedisStorageStateReader } from "./storage/stateReader";

const rrc = new ReliableRedisClient("TEST", "localhost", 6379);

(async () => {
    try {
        await rrc.start();
        const reader = new RedisStorageStateReader(rrc, "TEST");
        const keyId = "example";

        const unsub = await reader.fetchStateAndListen(keyId, (full) => {
            console.log("FULL", full);
        }, (diff) => {
            console.log("DIFF", diff);
        }, (error) => {
            console.log("ERROR", error);
        });
        
        
        setTimeout(() => {
            unsub();
        }, 60 * 1000);

    } catch(e: any) {
        console.error(e);
    }
})();

