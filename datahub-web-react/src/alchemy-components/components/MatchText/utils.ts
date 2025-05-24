export function annotateHighlightedText(input: string, highlight: string): { text: string; highlighted: boolean }[] {
    const escapedHighlight = highlight.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

    if (escapedHighlight === '')
        return [
            {
                text: input,
                highlighted: false,
            },
        ];

    const result: { text: string; highlighted: boolean }[] = [];
    // Case-insensitive regex for matching
    const regex = new RegExp(`(${escapedHighlight})`, 'gi');
    let lastIndex = 0;

    input.replace(regex, (match, p1, offset) => {
        if (offset > lastIndex) {
            result.push({ text: input.slice(lastIndex, offset), highlighted: false });
        }
        result.push({ text: p1, highlighted: true });
        lastIndex = offset + match.length;

        return match;
    });

    if (lastIndex < input.length) {
        result.push({ text: input.slice(lastIndex), highlighted: false });
    }

    return result;
}
