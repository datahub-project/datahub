/**
 * Types for parsed message content parts
 */
export interface MessagePart {
    type: 'markdown' | 'code';
    content: string;
    language?: string;
}

/**
 * Parses a message string to extract code blocks and regular markdown content.
 *
 * This function handles various AI message structures including:
 * - Regular markdown text
 * - Complete code blocks with language identifiers (e.g., ```sql)
 * - Incomplete/truncated code blocks (missing closing ```)
 * - Mixed content with multiple code blocks and markdown sections
 *
 * The parser is designed to handle streaming responses where code blocks
 * may arrive incomplete, allowing the UI to render partial content gracefully.
 *
 * @param text - The raw message text to parse
 * @returns Array of message parts, each with a type (markdown|code), content, and optional language
 *
 * @example
 * // SQL query with explanation
 * parseMessageContent("Here's a query:\n\n```sql\nSELECT * FROM users\n```\n\nThis returns all users.")
 * // Returns: [
 * //   { type: 'markdown', content: "Here's a query:" },
 * //   { type: 'code', content: 'SELECT * FROM users', language: 'sql' },
 * //   { type: 'markdown', content: 'This returns all users.' }
 * // ]
 *
 * @example
 * // Truncated code block (streaming in progress)
 * parseMessageContent("Query:\n```sql\nSELECT * FROM")
 * // Returns: [
 * //   { type: 'markdown', content: 'Query:' },
 * //   { type: 'code', content: 'SELECT * FROM', language: 'sql' }
 * // ]
 */
export function parseMessageContent(text: string): MessagePart[] {
    const parts: MessagePart[] = [];

    // Regex to match complete code blocks with optional language identifier
    // Matches: ```language\ncode\n``` or ```\ncode\n```
    const codeBlockRegex = /```(\w+)?[\s\n]*([\s\S]*?)```/g;
    let lastIndex = 0;
    let match = codeBlockRegex.exec(text);

    while (match !== null) {
        // Add markdown content before code block
        if (match.index > lastIndex) {
            const markdownText = text.substring(lastIndex, match.index);
            if (markdownText.trim()) {
                parts.push({ type: 'markdown', content: markdownText });
            }
        }

        // Add complete code block
        parts.push({
            type: 'code',
            language: match[1] || 'code',
            content: match[2].trim(),
        });

        lastIndex = match.index + match[0].length;
        match = codeBlockRegex.exec(text);
    }

    // Check for incomplete/truncated code block at the end (missing closing ```)
    // This handles streaming responses where code blocks may be incomplete
    const remainingText = text.substring(lastIndex);
    const incompleteCodeMatch = remainingText.match(/```(\w+)?[\s\n]*([\s\S]*?)$/);

    if (incompleteCodeMatch) {
        // Add markdown content before incomplete code block
        const beforeIncomplete = remainingText.substring(0, incompleteCodeMatch.index);
        if (beforeIncomplete.trim()) {
            parts.push({ type: 'markdown', content: beforeIncomplete });
        }

        // Add incomplete code block
        const codeContent = incompleteCodeMatch[2].trim();
        if (codeContent) {
            parts.push({
                type: 'code',
                language: incompleteCodeMatch[1] || 'code',
                content: codeContent,
            });
        }
    } else if (remainingText.trim()) {
        // No incomplete code block, just regular markdown
        parts.push({ type: 'markdown', content: remainingText });
    }

    // If no parts found, return the whole text as markdown
    if (parts.length === 0) {
        parts.push({ type: 'markdown', content: text });
    }

    return parts;
}
