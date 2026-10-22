import refractor from 'refractor';

/**
 * Remirror's code block reads the shared refractor instance and ships with
 * `supportedLanguages: []`. The `refractor` package entry registers every
 * grammar on that instance, which is what highlights fences already stored
 * as java, scala, go, and the rest of the catalog. File previews pass the
 * file extension through the same instance.
 *
 * Import this from the lazy editor only. Home and entity chunks keep the
 * smaller prism-light set used by the standalone code block.
 */
export function registerEditorCodeLanguages(): void {
    refractor.listLanguages();
}

/**
 * Languages the docs editor can highlight, including aliases refractor registers.
 *
 * @returns Sorted language ids from the shared refractor instance
 */
export function listEditorCodeLanguages(): readonly string[] {
    registerEditorCodeLanguages();
    return refractor
        .listLanguages()
        .slice()
        .sort((left, right) => left.localeCompare(right));
}
