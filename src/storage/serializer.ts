import { serializeObject, unserializeObject } from "deep-diff-patcher";

// We now use the object serializer from my deep-diff-patcher project, wrapped in JSON

export function unserialize<T>(str: string) : T {
    const parsed = JSON.parse(str);
    if (typeof parsed === "object" && parsed !== null) {
        return unserializeObject(parsed, false);
    } else {
        return parsed;
    }
}

export function serialize<T>(v: T) : string {
    if (typeof v === "object" && v !== null) {
        const s = serializeObject(v, false);
        return JSON.stringify(s);
    } else {
        return JSON.stringify(v);
    }
}
