// Kept out of DataHubMentionsExtension so read-only markdown rendering can recognize
// mention spans without loading Remirror.
export const DATAHUB_MENTION_ATTRS = {
    urn: 'data-datahub-mention-urn',
} as const;
