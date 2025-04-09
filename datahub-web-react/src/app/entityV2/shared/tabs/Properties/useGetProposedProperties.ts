import { ActionRequestType } from '@src/types.generated';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { getProposedItemsByType } from '../../utils';
import { mapStructuredPropertyToPropertyRow } from './useStructuredProperties';
import { PropertyRow, ValueColumnData } from './types';

interface Props {
    fieldPath?: string;
    propertyUrn?: string;
}

export const useGetProposedProperties = ({ fieldPath, propertyUrn }: Props = {}) => {
    const { entityData } = useEntityData();

    const proposedRequests = getProposedItemsByType(
        entityData?.proposals || [],
        ActionRequestType.StructuredPropertyAssociation,
    );

    const proposedProperties = fieldPath
        ? proposedRequests.flatMap(
              (request) =>
                  (request.subResource &&
                      request.subResource === fieldPath &&
                      request.params?.structuredPropertyProposal?.structuredProperties[0]?.structuredProperty?.exists &&
                      (propertyUrn
                          ? request.params?.structuredPropertyProposal?.structuredProperties[0]?.structuredProperty
                                ?.urn === propertyUrn
                          : true) &&
                      request.params?.structuredPropertyProposal?.structuredProperties) ||
                  [],
          )
        : proposedRequests.flatMap(
              (request) =>
                  (!request.subResource &&
                      request.params?.structuredPropertyProposal?.structuredProperties[0]?.structuredProperty.exists &&
                      request.params?.structuredPropertyProposal?.structuredProperties) ||
                  [],
          );

    const proposedRows: PropertyRow[] = proposedProperties.flatMap((property, index) => {
        const propertyRow = mapStructuredPropertyToPropertyRow(property, true);
        // need to add index to qualified name for unique keys in table
        return { ...propertyRow, qualifiedName: `${propertyRow.qualifiedName}-${index}` };
    });

    const proposedValues: ValueColumnData[] = proposedRows.flatMap((row) => row.values || []);

    return { proposedProperties, proposedRows, proposedValues };
};
