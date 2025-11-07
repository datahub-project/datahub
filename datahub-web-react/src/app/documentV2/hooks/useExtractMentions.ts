import { useMemo } from 'react';

/**
 * Hook to extract @ mentions (URNs) from markdown text.
 * Searches for patterns like @urn:li:dataset:... or @urn:li:document:...
 */
export const useExtractMentions = (content: string) => {
    const mentions = useMemo(() => {
        if (!content) return { documentUrns: [], assetUrns: [] };

        // Match @urn:... patterns
        // URNs typically follow the format: urn:li:entityType:...
        const urnPattern = /@(urn:li:[a-zA-Z]+:[^\s)}\]]+)/g;
        const matches = Array.from(content.matchAll(urnPattern));

        const documentUrns: string[] = [];
        const assetUrns: string[] = [];

        matches.forEach((match) => {
            const urn = match[1]; // Extract the URN without the @ symbol

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
