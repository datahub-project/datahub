import { useMemo } from 'react';

/**
 * Hook to extract @ mentions (URNs) from markdown text.
 * Searches for markdown link patterns like [@Entity](urn:li:entityType:id)
 */
export const useExtractMentions = (content: string) => {
    const mentions = useMemo(() => {
        if (!content) return { documentUrns: [], assetUrns: [] };

        // Match markdown link syntax: [text](urn:li:entityType:id)
        // Handle URNs with nested parentheses by matching everything between the markdown link's parens
        // The pattern matches: [text](urn:li:entityType:...) where ... can include nested parens
        // We match the URN prefix, then allow nested paren groups or non-paren characters (one or more)
        const urnPattern = /\[([^\]]+)\]\((urn:li:[a-zA-Z]+:(?:[^)(]+|\([^)]*\))+)\)/g;
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
