/**
 * Question words that indicate the user is asking a natural language question.
 * When a query starts with these words, we don't need to add a prefix.
 */
const QUESTION_WORDS = [
    'what',
    'which',
    'why',
    'how',
    'when',
    'where',
    'who',
    'does',
    'is',
    'are',
    'can',
    'could',
    'would',
    'should',
    'do',
    'has',
    'have',
    'will',
    'tell',
    'explain',
    'describe',
    'show',
    'find',
    'list',
    'give',
];

/**
 * Determines whether the query is already a natural language question.
 * A query is considered a question if:
 * - It ends with a question mark
 * - It starts with a common question word
 */
function isNaturalLanguageQuestion(query: string): boolean {
    const trimmedQuery = query.trim().toLowerCase();

    // Check if it ends with a question mark
    if (trimmedQuery.endsWith('?')) {
        return true;
    }

    // Check if it starts with a question word
    const firstWord = trimmedQuery.split(/\s+/)[0];
    return QUESTION_WORDS.includes(firstWord);
}

/**
 * Formats a search query for the Ask DataHub AI chat.
 *
 * If the query is already a natural language question (ends with "?" or starts
 * with question words like "what", "how", "why", etc.), returns it as-is.
 *
 * Otherwise, wraps it with "Help me find data related to {query}" to provide
 * context for the AI.
 *
 * @param query - The user's search query
 * @returns The formatted message for the AI chat
 */
export default function formatAskDataHubMessage(query: string): string {
    const trimmedQuery = query.trim();

    if (!trimmedQuery) {
        return '';
    }

    if (isNaturalLanguageQuestion(trimmedQuery)) {
        return trimmedQuery;
    }

    return `Help me find data related to ${trimmedQuery}`;
}
