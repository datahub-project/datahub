/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { useEffect, useState } from 'react';

import { getMatchedFieldsForList } from '@app/search/context/SearchResultContext';
import { PreviewSection } from '@app/shared/MatchesContext';

import { SearchResult } from '@types';

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
