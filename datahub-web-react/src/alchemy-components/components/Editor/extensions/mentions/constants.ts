// Kept out of DataHubMentionsExtension so read-only markdown rendering can recognize
// mention spans without loading Remirror.
export const DATAHUB_MENTION_ATTRS = {
    urn: 'data-datahub-mention-urn',
    /** Stable label for markdown serialization — node textContent can include icon glyphs. */
    name: 'data-datahub-mention-name',
} as const;
