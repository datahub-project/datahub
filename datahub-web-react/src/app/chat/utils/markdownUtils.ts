/**
 * Escape HTML special characters for HTML content
 * Note: Only escapes characters that are dangerous in HTML content (&, <, >)
 * Quotes and apostrophes don't need escaping in HTML content (only in attributes)
 * Escaping them causes cursor position issues due to browser entity decoding
 * @param text The text to escape
 * @returns HTML-safe string for content
 */
function escapeHtmlContent(text: string): string {
    return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

/**
 * Escape HTML special characters for HTML attributes
 * Includes quote escaping which is necessary in attribute values
 * @param text The text to escape
 * @returns HTML-safe string for attributes
 */
function escapeHtmlAttribute(text: string): string {
    return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
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
            result += escapeHtmlContent(markdown.substring(i));
            break;
        }

        // Add text before the mention (escaped)
        result += escapeHtmlContent(markdown.substring(i, mentionStart));

        // Find the closing ]
        const closingBracket = markdown.indexOf(']', mentionStart);
        if (closingBracket === -1) {
            // Malformed, treat rest as text (escaped)
            result += escapeHtmlContent(markdown.substring(mentionStart));
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
                result += escapeHtmlContent(markdown.substring(mentionStart));
                break;
            }

            // Extract the URN
            const urn = markdown.substring(closingBracket + 2, j - 1);

            // Create the mention span (escape urn for attribute, displayName for content)
            result += `<span class="mention" data-urn="${escapeHtmlAttribute(urn)}" contenteditable="false">@${escapeHtmlContent(displayName)}</span>`;

            i = j;
        } else {
            // Not a link, treat as text (escaped)
            result += escapeHtmlContent(markdown.substring(mentionStart, closingBracket + 1));
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

/**
 * Pre-process markdown to convert URN links to clickable spans.
 * MDEditor.Markdown sanitizes unknown URL schemes (like urn:) to javascript:void(0),
 * so we convert [text](urn:li:...) to <span class="urn-link" data-urn="...">text</span>
 * which MDEditor will preserve, and we handle clicks separately.
 * @param markdown The markdown string to process
 * @returns Markdown with URN links converted to spans
 */
export function convertUrnLinksToSpans(markdown: string): string {
    // Match markdown links: [text](url) - handle nested parentheses in URNs
    let result = '';
    let pos = 0;

    while (pos < markdown.length) {
        const linkStart = markdown.indexOf('[', pos);
        if (linkStart === -1) {
            result += markdown.substring(pos);
            break;
        }

        // Add text before the link
        result += markdown.substring(pos, linkStart);

        // Find the closing bracket
        const textEnd = markdown.indexOf(']', linkStart);
        if (textEnd === -1 || markdown[textEnd + 1] !== '(') {
            // Not a valid link syntax, add the bracket and move on
            result += markdown[linkStart];
            pos = linkStart + 1;
        } else {
            const linkText = markdown.substring(linkStart + 1, textEnd);

            // Find URL with balanced parentheses
            const urlStart = textEnd + 2;
            let parenCount = 0;
            let urlEnd = urlStart;

            while (urlEnd < markdown.length) {
                const char = markdown[urlEnd];
                if (char === '(') {
                    parenCount++;
                } else if (char === ')') {
                    if (parenCount === 0) break;
                    parenCount--;
                }
                urlEnd++;
            }

            if (urlEnd >= markdown.length) {
                result += markdown.substring(linkStart);
                break;
            }

            const url = markdown.substring(urlStart, urlEnd);

            // Check if this is a URN link
            if (url.startsWith('urn:li:') || url.startsWith('urn%3Ali%3A')) {
                // Convert to a span with data attribute (MDEditor preserves HTML)
                const escapedUrn = url.replace(/"/g, '&quot;');
                result += `<span class="urn-link" data-urn="${escapedUrn}">${linkText}</span>`;
            } else {
                // Keep non-URN links as-is
                result += `[${linkText}](${url})`;
            }

            pos = urlEnd + 1;
        }
    }

    return result;
}
