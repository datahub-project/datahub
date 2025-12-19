export type ConversationListItem = {
    urn: string;
    title: string | null;
    messageCount?: number;
    created?: { time?: number } | null;
    lastUpdated?: { time?: number } | null;
};

/**
 * Normalize server/optimistic conversation shapes for the list.
 */
export function mapToConversationListItem(conversation: any): ConversationListItem {
    const now = Date.now();
    return {
        urn: conversation?.urn,
        title: conversation?.title ?? null,
        messageCount: conversation?.messageCount ?? 0,
        created: conversation?.created ?? { time: now },
        lastUpdated: conversation?.lastUpdated ?? { time: now },
    };
}

/**
 * Type for conversations that can be sorted (only needs time fields)
 */
type SortableConversation = {
    lastUpdated?: { time?: number } | null;
    created?: { time?: number } | null;
};

/**
 * Sort conversations by most recent activity (lastUpdated time, fallback to created time).
 * Most recent conversations appear first.
 */
export function sortConversationsByMostRecent<T extends SortableConversation>(conversations: T[]): T[] {
    return [...conversations].sort((a, b) => {
        const aTime = a.lastUpdated?.time || a.created?.time || 0;
        const bTime = b.lastUpdated?.time || b.created?.time || 0;
        return bTime - aTime;
    });
}
