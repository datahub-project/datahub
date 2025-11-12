import { useMemo } from 'react';

/**
 * Hook to extract @ mentions (URNs) from markdown text.
 * Searches for markdown link patterns like [@Entity](urn:li:entityType:id)
 */
export const useExtractMentions = (content: string) => {
    const mentions = useMemo(() => {
        if (!content) return { documentUrns: [], assetUrns: [] };

        // Match markdown link syntax: [text](urn:li:entityType:id)
        const urnPattern = /\[([^\]]+)\]\((urn:li:[a-zA-Z]+:[^\s)]+)\)/g;
        const matches = Array.from(content.matchAll(urnPattern));

        const documentUrns: string[] = [];
        const assetUrns: string[] = [];

        matches.forEach((match) => {
            const urn = match[2]; // URN is in the second capture group (inside parentheses)

            // Check if it's a document URN
            if (urn.includes(':document:')) {
                if (!documentUrns.includes(urn)) {
                    documentUrns.push(urn);
                }
            } else if (!assetUrns.includes(urn)) {
                // Everything else is considered an asset
                assetUrns.push(urn);
            }
        });

        return { documentUrns, assetUrns };
    }, [content]);

    return mentions;
};
