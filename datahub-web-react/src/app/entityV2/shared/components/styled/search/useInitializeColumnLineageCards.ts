import { useContext, useEffect, useState } from 'react';
import { LineageTabContext } from '../../../tabs/Lineage/LineageTabContext';
import { PreviewSection } from '../../../../../shared/MatchesContext';
import { EntityType, SearchResult } from '../../../../../../types.generated';

export const useInitializeColumnLineageCards = (
    searchResults: SearchResult[],
    setUrnToExpandedSection: (urnToExpandedSection: Record<string, PreviewSection>) => void,
) => {
    const [isInitalized, setIsInitalized] = useState(false);
    const { isColumnLevelLineage, selectedColumn } = useContext(LineageTabContext);

    useEffect(() => {
        if (isColumnLevelLineage && selectedColumn && searchResults?.length && !isInitalized) {
            const updatedUrnToExpandedSection = {};
            searchResults.forEach((result) => {
                // only open datasets column paths expanded section because they're the only ones with columns
                if (result.entity.type === EntityType.Dataset) {
                    updatedUrnToExpandedSection[result.entity.urn] = PreviewSection.COLUMN_PATHS;
                }
            });
            setUrnToExpandedSection(updatedUrnToExpandedSection);
            setIsInitalized(true);
        }
    }, [isInitalized, searchResults, isColumnLevelLineage, selectedColumn, setUrnToExpandedSection]);
};
