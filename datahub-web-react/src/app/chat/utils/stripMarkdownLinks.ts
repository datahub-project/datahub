/**
 * Strip markdown links [text](url) -> text, handling nested parentheses (e.g., URNs).
 */
export function stripMarkdownLinks(text: string): string {
    let result = '';
    let i = 0;
    while (i < text.length) {
        let matched = false;
        if (text[i] === '[') {
            // Find the next unescaped closing bracket
            let closeBracket = i + 1;
            while (closeBracket < text.length) {
                if (text[closeBracket] === ']' && text[closeBracket - 1] !== '\\') {
                    break;
                }
                closeBracket += 1;
            }

            if (closeBracket < text.length && text[closeBracket] === ']' && text[closeBracket + 1] === '(') {
                // Found [text]( - now find matching ) by counting parens
                const linkText = text.slice(i + 1, closeBracket);
                let parenDepth = 1;
                let j = closeBracket + 2;
                while (j < text.length && parenDepth > 0) {
                    if (text[j] === '(') parenDepth++;
                    else if (text[j] === ')') parenDepth--;
                    j++;
                }
                if (parenDepth === 0) {
                    result += linkText;
                    i = j;
                    matched = true;
                }
            }
        }
        if (!matched) {
            result += text[i];
            i++;
        }
    }
    return result;
}
