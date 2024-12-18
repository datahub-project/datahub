import { useUserContext } from '@src/app/context/useUserContext';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { StyledList } from '@src/app/entity/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/FieldProperties';
import { EditColumn } from '@src/app/entity/shared/tabs/Properties/Edit/EditColumn';
import StructuredPropertyValue from '@src/app/entity/shared/tabs/Properties/StructuredPropertyValue';
import { PropertyRow } from '@src/app/entity/shared/tabs/Properties/types';
import EmptySectionText from '@src/app/shared/sidebar/EmptySectionText';
import {
    getDisplayName,
    getEntityTypesPropertyFilter,
    getNotHiddenPropertyFilter,
    getPropertyRowFromSearchResult,
} from '@src/app/govern/structuredProperties/utils';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType, SchemaField, SearchResult, StdDataType, StructuredPropertyEntity } from '@src/types.generated';
import {
    SHOW_IN_ASSET_SUMMARY_PROPERTY_FILTER_NAME,
    SHOW_IN_COLUMNS_TABLE_PROPERTY_FILTER_NAME,
} from '@src/app/search/utils/constants';
import {
    SectionHeader,
    StyledDivider,
} from '@src/app/entity/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/components';
import { useGetEntityWithSchema } from '@src/app/entity/shared/tabs/Dataset/Schema/useGetEntitySchema';
import React from 'react';
import { SidebarHeader } from '../SidebarHeader';

interface Props {
    properties?: {
        schemaField?: SchemaField;
        schemaColumnProperties?: SearchResult[];
    };
}

const SidebarStructuredPropsSection = ({ properties }: Props) => {
    const schemaField = properties?.schemaField;
    const schemaColumnProperties = properties?.schemaColumnProperties;
    const { entityData, entityType } = useEntityData();
    const me = useUserContext();
    const entityRegistry = useEntityRegistry();
    const { refetch: refetchSchema } = useGetEntityWithSchema(true);

    const currentProperties = schemaField
        ? schemaField?.schemaFieldEntity?.structuredProperties
        : entityData?.structuredProperties;

    const inputs = {
        types: [EntityType.StructuredProperty],
        query: '',
        start: 0,
        count: 50,
        searchFlags: { skipCache: true },
        orFilters: [
            {
                and: [
                    getEntityTypesPropertyFilter(entityRegistry, !!schemaField, entityType),
                    getNotHiddenPropertyFilter(),
                    {
                        field: schemaField
                            ? SHOW_IN_COLUMNS_TABLE_PROPERTY_FILTER_NAME
                            : SHOW_IN_ASSET_SUMMARY_PROPERTY_FILTER_NAME,
                        values: ['true'],
                    },
                ],
            },
        ],
    };

    // Execute search
    const { data } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: inputs,
        },
        skip: !!schemaColumnProperties,
        fetchPolicy: 'cache-first',
    });

    const entityTypeProperties = schemaColumnProperties || data?.searchAcrossEntities?.searchResults;

    const canEditProperties = me.platformPrivileges?.manageStructuredProperties;

    return (
        <>
            {entityTypeProperties?.map((property) => {
                const propertyRow: PropertyRow | undefined = getPropertyRowFromSearchResult(
                    property,
                    currentProperties,
                );
                const isRichText = propertyRow?.dataType?.info?.type === StdDataType.RichText;
                const values = propertyRow?.values;
                const hasMultipleValues = values && values.length > 1;
                const propertyName = getDisplayName(property.entity as StructuredPropertyEntity);

                return (
                    <>
                        <div>
                            <SidebarHeader
                                title={propertyName}
                                titleComponent={<SectionHeader>{propertyName}</SectionHeader>}
                                actions={
                                    canEditProperties && (
                                        <>
                                            <EditColumn
                                                structuredProperty={property.entity as StructuredPropertyEntity}
                                                values={values?.map((v) => v.value) || []}
                                                isAddMode={!values}
                                                associatedUrn={schemaField?.schemaFieldEntity?.urn}
                                                refetch={schemaField ? refetchSchema : undefined}
                                            />
                                        </>
                                    )
                                }
                            />

                            {values ? (
                                <>
                                    {hasMultipleValues ? (
                                        <StyledList>
                                            {values.map((value) => (
                                                <li>
                                                    <StructuredPropertyValue value={value} isRichText={isRichText} />
                                                </li>
                                            ))}
                                        </StyledList>
                                    ) : (
                                        <>
                                            {values?.map((value) => (
                                                <StructuredPropertyValue value={value} isRichText={isRichText} />
                                            ))}
                                        </>
                                    )}
                                </>
                            ) : (
                                <EmptySectionText message="No value set" />
                            )}
                        </div>
                        {schemaField && <StyledDivider />}
                    </>
                );
            })}
        </>
    );
};

export default SidebarStructuredPropsSection;
