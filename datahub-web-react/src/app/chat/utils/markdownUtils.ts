/**
 * Escape HTML special characters to prevent XSS attacks
 * Properly escapes characters for both HTML content and attributes
 * @param text The text to escape
 * @returns HTML-safe string
 */
function escapeHtml(text: string): string {
    return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    // Note: Apostrophes don't need escaping in HTML content (only in attributes)
    // Escaping them causes cursor position issues due to browser entity decoding
}

/**
 * Convert markdown to HTML for display
 * @param markdown The markdown string to convert
 * @returns HTML string with mention spans
 */
export function markdownToHtml(markdown: string): string {
    let result = '';
    let i = 0;

    while (i < markdown.length) {
        // Look for the start of a mention [@
        const mentionStart = markdown.indexOf('[@', i);

        if (mentionStart === -1) {
            // No more mentions, add remaining text (escaped)
            result += escapeHtml(markdown.substring(i));
            break;
        }

        // Add text before the mention (escaped)
        result += escapeHtml(markdown.substring(i, mentionStart));

        // Find the closing ]
        const closingBracket = markdown.indexOf(']', mentionStart);
        if (closingBracket === -1) {
            // Malformed, treat rest as text (escaped)
            result += escapeHtml(markdown.substring(mentionStart));
            break;
        }

        // Extract display name
        const displayName = markdown.substring(mentionStart + 2, closingBracket);

        // Check for opening parenthesis
        if (markdown[closingBracket + 1] === '(') {
            // Find matching closing parenthesis (handling nesting)
            let parenDepth = 1;
            let j = closingBracket + 2;
            while (j < markdown.length && parenDepth > 0) {
                if (markdown[j] === '(') {
                    parenDepth++;
                } else if (markdown[j] === ')') {
                    parenDepth--;
                }
                j++;
            }

            if (parenDepth !== 0) {
                // Malformed, treat as text (escaped)
                result += escapeHtml(markdown.substring(mentionStart));
                break;
            }

            // Extract the URN
            const urn = markdown.substring(closingBracket + 2, j - 1);

            // Create the mention span (escape urn and displayName to prevent XSS)
            result += `<span class="mention" data-urn="${escapeHtml(urn)}" contenteditable="false">@${escapeHtml(displayName)}</span>`;

            i = j;
        } else {
            // Not a link, treat as text (escaped)
            result += escapeHtml(markdown.substring(mentionStart, closingBracket + 1));
            i = closingBracket + 1;
        }
    }

    return result;
}

/**
 * Convert HTML back to markdown
 * @param html The HTML string to convert
 * @returns Markdown string with mention links
 */
export function htmlToMarkdown(html: string): string {
    const div = document.createElement('div');
    div.innerHTML = html;

    // Replace mention spans with markdown
    const mentions = div.querySelectorAll('.mention');
    mentions.forEach((mention) => {
        const displayName = mention.textContent?.replace('@', '') || '';
        const urn = mention.getAttribute('data-urn') || '';
        const markdown = `[@${displayName}](${urn})`;
        const textNode = document.createTextNode(markdown);
        mention.replaceWith(textNode);
    });

    return div.textContent || '';
}

/**
 * Get cursor position in text content
 * @param element The content editable element
 * @returns The cursor position in characters
 */
export function getCursorPosition(element: HTMLDivElement): number {
    const selection = window.getSelection();
    if (!selection || selection.rangeCount === 0) return 0;

    const range = selection.getRangeAt(0);
    const preCaretRange = range.cloneRange();
    preCaretRange.selectNodeContents(element);
    preCaretRange.setEnd(range.endContainer, range.endOffset);

    return preCaretRange.toString().length;
}

/**
 * Set cursor position in content editable
 * @param element The content editable element
 * @param position The position to set the cursor to
 */
export function setCursorPosition(element: HTMLDivElement, position: number) {
    const selection = window.getSelection();
    if (!selection) return;

    let currentPos = 0;
    const walker = document.createTreeWalker(element, NodeFilter.SHOW_TEXT, null);

    let node = walker.nextNode();
    while (node) {
        const nodeLength = node.textContent?.length || 0;
        if (currentPos + nodeLength >= position) {
            const range = document.createRange();
            range.setStart(node, position - currentPos);
            range.collapse(true);
            selection.removeAllRanges();
            selection.addRange(range);
            return;
        }
        currentPos += nodeLength;
        node = walker.nextNode();
    }
}
