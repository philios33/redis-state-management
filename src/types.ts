export interface IMessage<T,M> {
    type: T
    meta: M
    occurredAt: string

    // Note: No identifier here
    // Messages are too ephemeral to bother giving them ids, plus we don't really care about a message id.

    // Note: No order key here
    // Order is the order they are stored in the queue implementation, we don't need to redundantly store this.
}

export type GenericMessage = IMessage<string, any>;

export type GenericMessageWithId = {
    id: string // This IS necessary to allow for message confirmation, but is left up to the implementation to store or not.
    message: GenericMessage
}

export type StateVersion<T> = {
    version: number
    writtenAt: string
    value: T
}

export type DiffMessage = {
    fromVersion: number
    toVersion: number
    writtenAt: string
    deltaPayload: any
}