import { PropertyTableRow } from '@src/app/entity/shared/tabs/Properties/types';
import { PropertyRow } from './tabs/Properties/types';

export const useGetOnlyProposalRows = (dataSource: PropertyTableRow[], proposedRows: PropertyRow[]) => {
    const unmatchedProposedRows = proposedRows.filter(
        (proposedRow) =>
            !dataSource.some((row) => row.mainRow?.structuredProperty?.urn === proposedRow.structuredProperty?.urn),
    );

    // Group the proposed values by property
    const groupedMap = new Map<string, PropertyTableRow>();

    unmatchedProposedRows.forEach((proposedRow) => {
        if (proposedRow.structuredProperty?.urn) {
            if (!groupedMap.has(proposedRow.structuredProperty?.urn)) {
                groupedMap.set(proposedRow.structuredProperty?.urn, { mainRow: undefined, proposedRows: [] });
            }
            groupedMap.get(proposedRow.structuredProperty?.urn)?.proposedRows?.push(proposedRow);
        }
    });

    return Array.from(groupedMap.values());
};
