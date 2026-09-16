import { useMemo } from 'react';

import { extractMentions } from '@app/document/utils/extractMentions';

/**
 * Hook to extract @ mentions (URNs) from markdown text.
 * Searches for markdown link patterns like [@Entity](urn:li:entityType:id)
 *
 * This hook memoizes the result of extractMentions to avoid recalculating on every render.
 */
export const useExtractMentions = (content: string) => {
    return useMemo(() => extractMentions(content), [content]);
};
