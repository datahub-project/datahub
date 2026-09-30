import { PencilSimple } from '@phosphor-icons/react/dist/csr/PencilSimple';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import React, { useState } from 'react';

import { EMPTY_MESSAGES } from '@app/entityV2/shared/constants';
import EmptySectionText from '@app/entityV2/shared/containers/profile/sidebar/EmptySectionText';
import SectionActionButton from '@app/entityV2/shared/containers/profile/sidebar/SectionActionButton';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { getSidebarStructuredPropertiesOrFilters } from '@app/entityV2/shared/sidebarSection/utils';
import { StyledDivider } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/components';
import StructuredPropertyValueList from '@app/entityV2/shared/tabs/Properties/StructuredPropertyValueList';
import { PropertyRow } from '@app/entityV2/shared/tabs/Properties/types';
import { useReloadableQuery } from '@app/sharedV2/reloadableContext/hooks/useReloadableQuery';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import EditStructuredPropertyModal from '@src/app/entity/shared/tabs/Properties/Edit/EditStructuredPropertyModal';
import {
    getDisplayName,
    getPropertyRowFromSearchResult,
    matchesAllowedPlatforms,
} from '@src/app/govern/structuredProperties/utils';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import {
    DataPlatform,
    EntityType,
    Maybe,
    SchemaFieldEntity,
    SearchResult,
    StdDataType,
    StructuredPropertyEntity,
} from '@src/types.generated';

interface FieldProperties {
    isSchemaSidebar?: boolean;
    refetch?: () => void;
    fieldEntity?: Maybe<SchemaFieldEntity>;
}

interface Props {
    properties?: FieldProperties;
}

const MAX_STRUCTURED_PROPERTIES_TO_FETCH = 100;

const SidebarStructuredProperties = ({ properties }: Props) => {
    const { entityData, entityType } = useEntityData();
    const entityRegistry = useEntityRegistryV2();
    const canEditProps = entityData?.parent?.privileges?.canEditProperties || entityData?.privileges?.canEditProperties;
    const [isPropModalVisible, setIsPropModalVisible] = useState(false);
    const [selectedProperty, setSelectedProperty] = useState<SearchResult | undefined>();
    const isSchemaSidebar = properties?.isSchemaSidebar || false;

    const orFilters = getSidebarStructuredPropertiesOrFilters(isSchemaSidebar, entityRegistry, entityType);
    const inputs = {
        types: [EntityType.StructuredProperty],
        query: '',
        start: 0,
        count: MAX_STRUCTURED_PROPERTIES_TO_FETCH,
        searchFlags: { skipCache: true },
        orFilters,
    };

    // Execute search

    const { data } = useReloadableQuery(
        useGetSearchResultsForMultipleQuery,
        {
            type: getReloadableKeyType(ReloadableKeyTypeNamespace.STRUCTURED_PROPERTY, 'EntitySummaryTabSidebar'),
            id: `${entityType}-${isSchemaSidebar ? 'schema' : 'entity'}-sidebar`,
        },
        {
            variables: {
                input: inputs,
            },
            fetchPolicy: 'cache-first',
        },
    );

    // Determine the current entity's platform URN for filtering allowedPlatforms
    const platformUrn = isSchemaSidebar
        ? ((properties?.fieldEntity?.parent as { platform?: DataPlatform } | undefined)?.platform?.urn ??
          (entityData?.platform as DataPlatform | undefined)?.urn)
        : (entityData?.platform as DataPlatform | undefined)?.urn;

    const entityTypeProperties = data?.searchAcrossEntities?.searchResults?.filter((result) =>
        matchesAllowedPlatforms(result.entity as StructuredPropertyEntity, platformUrn),
    );

    const allProperties = isSchemaSidebar
        ? properties?.fieldEntity?.structuredProperties
        : entityData?.structuredProperties;
    // The entity (or schema field) whose values are shown; part of the value list's key so its
    // filter and paging state reset when the user moves to another asset.
    const scopeUrn = isSchemaSidebar ? properties?.fieldEntity?.urn : entityData?.urn;

    const selectedPropertyValues = selectedProperty
        ? getPropertyRowFromSearchResult(selectedProperty, allProperties)?.values
        : undefined;

    return (
        <>
            {entityTypeProperties?.map((property) => {
                const structuredProperty = property.entity as StructuredPropertyEntity;
                const propertyRow: PropertyRow | undefined = getPropertyRowFromSearchResult(property, allProperties);
                const isRichText = propertyRow?.dataType?.info?.type === StdDataType.RichText;
                const values = propertyRow?.values;
                const propertyName = getDisplayName(structuredProperty);
                const shouldHideIfPropertyIsEmpty = structuredProperty.settings?.hideInAssetSummaryWhenEmpty;

                if (!isSchemaSidebar && shouldHideIfPropertyIsEmpty && !values) {
                    return null;
                }

                return (
                    <>
                        <SidebarSection
                            title={propertyName}
                            key={property.entity.urn}
                            content={
                                <>
                                    {values && propertyRow ? (
                                        <StructuredPropertyValueList
                                            key={`${scopeUrn}:${property.entity.urn}`}
                                            propertyRow={propertyRow}
                                            isRichText={isRichText}
                                            renderValue={(_, node) => node}
                                            dataTestId={(val) => `property-${propertyName}-value-${val.value}`}
                                        />
                                    ) : (
                                        <EmptySectionText message={EMPTY_MESSAGES.structuredProps.title} />
                                    )}
                                </>
                            }
                            extra={
                                <>
                                    <SectionActionButton
                                        icon={values ? PencilSimple : Plus}
                                        onClick={(event) => {
                                            setSelectedProperty(property);
                                            setIsPropModalVisible(true);
                                            event.stopPropagation();
                                        }}
                                        actionPrivilege={!!canEditProps}
                                        dataTestId={`${propertyName}-add-or-edit-button`}
                                    />
                                </>
                            }
                        />
                        {isSchemaSidebar && <StyledDivider dashed />}
                    </>
                );
            })}

            {selectedProperty && (
                <EditStructuredPropertyModal
                    isOpen={isPropModalVisible}
                    closeModal={() => {
                        setIsPropModalVisible(false);
                        setSelectedProperty(undefined);
                    }}
                    structuredProperty={selectedProperty?.entity as StructuredPropertyEntity}
                    isAddMode={!selectedPropertyValues}
                    values={selectedPropertyValues?.map((val) => val.value)}
                    refetch={isSchemaSidebar ? properties?.refetch : undefined}
                    associatedUrn={isSchemaSidebar ? properties?.fieldEntity?.urn : undefined}
                />
            )}
        </>
    );
};

export default SidebarStructuredProperties;
