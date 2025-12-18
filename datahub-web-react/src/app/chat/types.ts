export interface ChatFeatureFlags {
    verboseMode: boolean;
}

export enum ChatVariant {
    Full = 'full',
    Compact = 'compact',
}

export enum ChatMessageAction {
    Copy = 'copy',
    OpenInChat = 'openInChat',
}
