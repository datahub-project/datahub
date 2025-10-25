// no-op imports removed; keep file lean

export interface MentionState {
    isActive: boolean;
    query: string;
    startIndex: number;
    coordinates: { top: number; left: number };
}

/**
 * Check if there's an active mention at the cursor position
 * @param textContent The text content of the input
 * @param cursorPos The current cursor position
 * @returns Object with mention info or null if no active mention
 */
export function getActiveMention(
    textContent: string,
    cursorPos: number,
): {
    startIndex: number;
    query: string;
} | null {
    const textBeforeCursor = textContent.substring(0, cursorPos);
    const lastAtIndex = textBeforeCursor.lastIndexOf('@');

    if (lastAtIndex === -1) return null;

    const textAfterAt = textBeforeCursor.substring(lastAtIndex + 1);

    // Check if there's no space or newline after @ (active mention)
    if (textAfterAt.includes(' ') || textAfterAt.includes('\n')) {
        return null;
    }

    // If the query is empty (cursor is right after @), it's still an active mention
    if (textAfterAt === '') {
        return {
            startIndex: lastAtIndex,
            query: '',
        };
    }

    return {
        startIndex: lastAtIndex,
        query: textAfterAt,
    };
}
