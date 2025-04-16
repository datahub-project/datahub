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
        ? proposedRequests.flatMap((request) => {
              if (request.subResource && request.subResource === fieldPath) {
                  let properties = request.params?.structuredPropertyProposal?.structuredProperties?.filter(
                      (prop) => prop.structuredProperty.exists,
                  );
                  if (propertyUrn) {
                      properties = properties?.filter((prop) => prop.structuredProperty.urn === propertyUrn);
                  }
                  return properties || [];
              }
              return [];
          })
        : proposedRequests.flatMap((request) => {
              if (!request.subResource) {
                  let properties = request.params?.structuredPropertyProposal?.structuredProperties?.filter(
                      (prop) => prop.structuredProperty.exists,
                  );
                  if (propertyUrn) {
                      properties = properties?.filter((prop) => prop.structuredProperty.urn === propertyUrn);
                  }
                  return properties || [];
              }
              return [];
          });

    const proposedRows: PropertyRow[] = proposedProperties.flatMap((property, index) => {
        const propertyRow = mapStructuredPropertyToPropertyRow(property, true);
        // need to add index to qualified name for unique keys in table
        return { ...propertyRow, qualifiedName: `${propertyRow.qualifiedName}-${index}` };
    });

    const proposedValues: ValueColumnData[] = proposedRows.flatMap((row) => row.values || []);

    return { proposedProperties, proposedRows, proposedValues };
};
