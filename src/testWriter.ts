import { ReliableRedisClient } from "./reliableRedisClient";
import { RedisStorageStateWriter } from "./storage/stateWriter";

const rrc = new ReliableRedisClient("TEST", "localhost", 6379);

(async () => {
    try {
        await rrc.start();
        const writer = new RedisStorageStateWriter(rrc, "TEST", "in");
        const keyId = "example";
        setInterval(async () => {
            const r = Math.round(Math.random() * 10);
            const obj = {
                phil: "great",
                num: r,
            }
            
            try {
                console.log("Writing", obj);
                await writer.writeStateObj(keyId, obj);
                console.log("Success");
            } catch(e: any) {
                // Ignore
                console.log("Failed: " + e.message);
            }
        }, 5 * 1000);
    } catch(e: any) {
        console.error(e);
    }
})();

