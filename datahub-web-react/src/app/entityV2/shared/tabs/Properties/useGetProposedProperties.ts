import { mapStructuredPropertyToPropertyRow } from '@app/entityV2/shared/tabs/Properties/useStructuredProperties';
import { getProposedItemsByType } from '@app/entityV2/shared/utils';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { ActionRequestType } from '@src/types.generated';

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
                  return (properties || []).map((prop) => ({
                      ...prop,
                      request,
                  }));
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
                  return (properties || []).map((prop) => ({
                      ...prop,
                      request,
                  }));
              }
              return [];
          });

    const proposedRows = proposedProperties.flatMap((property, index) => {
        const propertyRow = mapStructuredPropertyToPropertyRow(property, true);
        // need to add index to qualified name for unique keys in table
        return {
            ...propertyRow,
            qualifiedName: `${propertyRow.qualifiedName}-${index}`,
            request: property.request,
        };
    });

    const proposedValues = proposedRows.flatMap((row) =>
        (row.values || []).map((value) => ({
            value,
            request: row.request,
        })),
    );

    return { proposedProperties, proposedRows, proposedValues };
};
