import { useEffect, useState } from 'react';
import { PreviewSection } from '../../../../../shared/MatchesContext';
import { SearchResult } from '../../../../../../types.generated';
import { getMatchedFieldsForList } from '../../../../../search/context/SearchResultContext';

export const useInitializeSearchResultCards = (
    searchResults: SearchResult[],
    setUrnToExpandedSection: React.Dispatch<React.SetStateAction<Record<string, PreviewSection>>>,
) => {
    const [initializedUrns, setInitializedUrns] = useState<Set<string>>(new Set());

    useEffect(() => {
        if (searchResults?.length) {
            const updatedUrnToExpandedSection: Record<string, PreviewSection> = {};
            searchResults.forEach((result) => {
                if (!initializedUrns.has(result.entity.urn)) {
                    const matchedFields = getMatchedFieldsForList(
                        'fieldLabels',
                        result.entity.type,
                        result.matchedFields,
                    );
                    if (matchedFields.length > 0) {
                        updatedUrnToExpandedSection[result.entity.urn] = PreviewSection.MATCHES;
                        setInitializedUrns((urns) => {
                            const updatedUrns = new Set(urns);
                            updatedUrns.add(result.entity.urn);
                            return updatedUrns;
                        });
                    }
                }
            });
            setUrnToExpandedSection((prev) => ({ ...prev, ...updatedUrnToExpandedSection }));
        }
    }, [searchResults, setUrnToExpandedSection, initializedUrns, setInitializedUrns]);
};
