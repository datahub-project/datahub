export function getAutoCompleteEntityText(displayName: string, query: string) {
    const isPrefixMatch = displayName.toLowerCase().startsWith(query.toLowerCase());
    const matchedText = isPrefixMatch ? displayName.substring(0, query.length) : '';
    const unmatchedText = isPrefixMatch ? displayName.substring(query.length, displayName.length) : displayName;

    return { matchedText, unmatchedText };
}
