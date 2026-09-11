export function shouldShowDocumentSearchResults(isSearching: boolean, rootDocumentCount: number): boolean {
    return isSearching || rootDocumentCount === 0;
}
