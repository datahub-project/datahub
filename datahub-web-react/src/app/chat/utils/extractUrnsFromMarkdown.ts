/**
 * Extracts DataHub entity URNs from markdown text.
 * Supports both formats:
 * - Direct URN: [Entity Name](urn:li:dataset:...)
 * - Full URL: [Entity Name](https://domain.com/dataset/urn:li:dataset:...)
 */

// More sophisticated pattern that handles deeply nested URNs
// This uses a recursive approach to match balanced parentheses
const URN_PATTERN_BALANCED = /urn:li:[a-zA-Z]+:(?:[^,\s()]+|\((?:[^()]|\([^)]*\))*\))/g;

/**
 * Extracts markdown links with proper handling of nested parentheses
 * @param text The text to parse
 * @returns Array of matches with text and url
 */
function extractMarkdownLinks(text: string): Array<{ text: string; url: string }> {
    const links: Array<{ text: string; url: string }> = [];
    let pos = 0;

    while (pos < text.length) {
        // Find the start of a markdown link
        const linkStart = text.indexOf('[', pos);
        if (linkStart === -1) break;

        // Find the end of the link text
        const textEnd = text.indexOf(']', linkStart);
        if (textEnd === -1) {
            pos = linkStart + 1;
        } else if (text[textEnd + 1] !== '(') {
            pos = textEnd + 1;
        } else {
            // Extract the link text
            const linkText = text.substring(linkStart + 1, textEnd);

            // Find the URL with balanced parentheses
            const urlStart = textEnd + 2;
            let parenCount = 0;
            let urlEnd = urlStart;

            while (urlEnd < text.length) {
                const char = text[urlEnd];
                if (char === '(') {
                    parenCount++;
                } else if (char === ')') {
                    if (parenCount === 0) {
                        break;
                    }
                    parenCount--;
                }
                urlEnd++;
            }

            if (urlEnd < text.length) {
                const url = text.substring(urlStart, urlEnd);
                links.push({ text: linkText, url });
            }

            pos = urlEnd + 1;
        }
    }

    return links;
}

export interface ExtractedReference {
    text: string;
    urn: string;
}

/**
 * Extracts all entity references from markdown text
 * @param markdown The markdown text to parse
 * @returns Array of extracted references with text and URN
 */
export function extractReferencesFromMarkdown(markdown: string): ExtractedReference[] {
    if (!markdown) return [];

    const seenUrns = new Set<string>();

    // Find all markdown links using our custom parser
    const linkMatches = extractMarkdownLinks(markdown);

    const references = linkMatches
        .map((match) => {
            const { text, url } = match;

            // Decode URL first in case URNs are URL-encoded
            let decodedUrl = url;
            try {
                decodedUrl = decodeURIComponent(url);
            } catch (e) {
                // If decode fails, use original
                decodedUrl = url;
            }

            // Extract URNs from the decoded URL using the balanced pattern
            // This handles deeply nested URNs with proper parentheses balancing
            const allUrns = decodedUrl.match(URN_PATTERN_BALANCED);

            if (allUrns && allUrns.length > 0) {
                // Return the first (most complete) URN found
                return { text, urn: allUrns[0] };
            }
            return null;
        })
        .filter((ref): ref is ExtractedReference => {
            // Filter out nulls and duplicates
            if (!ref) return false;
            if (seenUrns.has(ref.urn)) return false;
            seenUrns.add(ref.urn);
            return true;
        });

    return references;
}

/**
 * Extracts just the URNs from markdown text (no display names)
 * @param markdown The markdown text to parse
 * @returns Array of unique URNs
 */
export function extractUrnsFromMarkdown(markdown: string): string[] {
    const references = extractReferencesFromMarkdown(markdown);
    return references.map((ref) => ref.urn);
}
