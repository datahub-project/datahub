import AddRoundedIcon from '@mui/icons-material/AddRounded';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import React, { useState } from 'react';

import { EMPTY_MESSAGES } from '@app/entityV2/shared/constants';
import EmptySectionText from '@app/entityV2/shared/containers/profile/sidebar/EmptySectionText';
import SectionActionButton from '@app/entityV2/shared/containers/profile/sidebar/SectionActionButton';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { getSidebarStructuredPropertiesOrFilters } from '@app/entityV2/shared/sidebarSection/utils';
import { StyledDivider } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/components';
import StructuredPropertyValue from '@app/entityV2/shared/tabs/Properties/StructuredPropertyValue';
import { PropertyRow } from '@app/entityV2/shared/tabs/Properties/types';
import { useGetProposedProperties } from '@app/entityV2/shared/tabs/Properties/useGetProposedProperties';
import { useHydratedEntityMap } from '@app/entityV2/shared/tabs/Properties/useHydratedEntityMap';
import ProposalModal from '@app/shared/tags/ProposalModal';
import { useReloadableQuery } from '@app/sharedV2/reloadableContext/hooks/useReloadableQuery';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import EditStructuredPropertyModal from '@src/app/entity/shared/tabs/Properties/Edit/EditStructuredPropertyModal';
import { getDisplayName, getPropertyRowFromSearchResult } from '@src/app/govern/structuredProperties/utils';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import {
    ActionRequest,
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
    const canEditProps =
        !!entityData?.parent?.privileges?.canEditProperties || !!entityData?.privileges?.canEditProperties;
    const canProposeProps =
        !!entityData?.parent?.privileges?.canProposeStructuredProperties ||
        !!entityData?.privileges?.canProposeStructuredProperties;
    const canEditSchemaFieldProps =
        !!entityData?.parent?.privileges?.canEditSchemaFieldStructuredProperties ||
        !!entityData?.privileges?.canEditSchemaFieldStructuredProperties;
    const canProposeSchemaFieldProps =
        !!entityData?.parent?.privileges?.canProposeSchemaFieldStructuredProperties ||
        !!entityData?.privileges?.canProposeSchemaFieldStructuredProperties;
    const [isPropModalVisible, setIsPropModalVisible] = useState(false);
    const [selectedProperty, setSelectedProperty] = useState<SearchResult | undefined>();
    const isSchemaSidebar = properties?.isSchemaSidebar || false;
    const [selectedActionRequest, setSelectedActionRequest] = useState<ActionRequest | undefined | null>(null);

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

    const entityTypeProperties = data?.searchAcrossEntities?.searchResults;

    const allProperties = isSchemaSidebar
        ? properties?.fieldEntity?.structuredProperties
        : entityData?.structuredProperties;

    const { proposedRows } = useGetProposedProperties({
        fieldPath: properties?.fieldEntity?.fieldPath,
    });

    const selectedPropertyValues = selectedProperty
        ? getPropertyRowFromSearchResult(selectedProperty, allProperties)?.values
        : undefined;

    const uniqueEntityUrnsToHydrate =
        entityTypeProperties
            ?.flatMap((property) => {
                const propertyRow: PropertyRow | undefined = getPropertyRowFromSearchResult(property, allProperties);
                const values = propertyRow?.values;
                if (!values) return [];
                return values?.map((value) => value?.entity?.urn);
            })
            .filter(Boolean) ?? [];

    const proposedEntityUrns = proposedRows
        .flatMap((row) => {
            const { values } = row;
            return values?.map((value) => value.entity?.urn);
        })
        .filter(Boolean);

    const hydratedEntityMap = useHydratedEntityMap(uniqueEntityUrnsToHydrate?.concat(proposedEntityUrns));

    return (
        <>
            {entityTypeProperties?.map((property) => {
                const structuredProperty = property.entity as StructuredPropertyEntity;
                const propertyRow: PropertyRow | undefined = getPropertyRowFromSearchResult(property, allProperties);
                const values = propertyRow?.values;
                const propertyName = getDisplayName(structuredProperty);
                const proposedPropRows = proposedRows.filter(
                    (row) => row.structuredProperty?.urn === property.entity.urn,
                );

                const isRichText =
                    propertyRow?.dataType?.info?.type === StdDataType.RichText ||
                    proposedPropRows[0]?.dataType?.info?.type === StdDataType.RichText;

                const proposedValues = proposedPropRows.flatMap((row) =>
                    (row.values || []).map((value) => ({
                        value,
                        request: row.request,
                    })),
                );

                // Hide property if configured to hide when empty and no values exist
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
                                    {values && (
                                        <>
                                            {values.map((val) => (
                                                <StructuredPropertyValue
                                                    value={val}
                                                    isRichText={isRichText}
                                                    hydratedEntityMap={hydratedEntityMap}
                                                    attribution={propertyRow.attribution}
                                                />
                                            ))}
                                        </>
                                    )}
                                    {proposedValues.length > 0 && (
                                        <>
                                            {proposedValues.map((val) => (
                                                <span
                                                    // eslint needs keyboard listener for non-interactive elements
                                                    role="button"
                                                    tabIndex={0}
                                                    onClick={() => {
                                                        setSelectedActionRequest(val.request);
                                                    }}
                                                    onKeyDown={(e) => {
                                                        if (e.key === 'Enter' || e.key === ' ') {
                                                            e.preventDefault();
                                                            setSelectedActionRequest(val.request);
                                                        }
                                                    }}
                                                >
                                                    <StructuredPropertyValue
                                                        value={val.value}
                                                        isRichText={isRichText}
                                                        hydratedEntityMap={hydratedEntityMap}
                                                        isProposed
                                                    />
                                                </span>
                                            ))}
                                        </>
                                    )}
                                    {!values && proposedValues.length === 0 && (
                                        <EmptySectionText message={EMPTY_MESSAGES.structuredProps.title} />
                                    )}
                                </>
                            }
                            extra={
                                <>
                                    <SectionActionButton
                                        button={values ? <EditOutlinedIcon /> : <AddRoundedIcon />}
                                        onClick={(event) => {
                                            setSelectedProperty(property);
                                            setIsPropModalVisible(true);
                                            event.stopPropagation();
                                        }}
                                        actionPrivilege={canEditProps || canProposeProps}
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
                    fieldEntity={isSchemaSidebar ? properties?.fieldEntity : undefined}
                    canEdit={isSchemaSidebar ? canEditSchemaFieldProps : canEditProps}
                    canPropose={isSchemaSidebar ? canProposeSchemaFieldProps : canProposeProps}
                />
            )}
            {selectedActionRequest && (
                <ProposalModal
                    actionRequest={selectedActionRequest}
                    selectedActionRequest={selectedActionRequest}
                    setSelectedActionRequest={setSelectedActionRequest}
                    refetch={isSchemaSidebar ? properties?.refetch : undefined}
                />
            )}
        </>
    );
};

export default SidebarStructuredProperties;
