import { TagsAndTermsSelected } from '@app/automations/types';

// Unmerges the tags, glossaryTerms, and glossaryNodes from an action's recipe
// [
//     "urn:li:glossaryTerm:AccountBalance",
//     "urn:li:tag:Legacy",
//     "urn:li:tag:__default_high_queries",
//     "urn:li:glossaryNode:ClientsAndAccounts"
// ]
export const unmergeTagsAndTerms = (terms: string[], nodes: string[], tags: string[]) => {
    const unmerged: TagsAndTermsSelected = { terms: [], tags: [], nodes: [] };

    if (!terms && !tags && !nodes) return unmerged;

    terms.forEach((term) => {
        unmerged.terms.push(term);
    });

    nodes.forEach((node) => {
        unmerged.nodes.push(node);
    });

    tags.forEach((tag) => {
        unmerged.tags.push(tag);
    });

    return unmerged;
};
