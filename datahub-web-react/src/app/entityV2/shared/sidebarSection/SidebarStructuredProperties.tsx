import AddRoundedIcon from '@mui/icons-material/AddRounded';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import { useUserContext } from '@src/app/context/useUserContext';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import EditStructuredPropertyModal from '@src/app/entity/shared/tabs/Properties/Edit/EditStructuredPropertyModal';
import {
    getDisplayName,
    getEntityTypesPropertyFilter,
    getNotHiddenPropertyFilter,
    getPropertyRowFromSearchResult,
} from '@src/app/govern/structuredProperties/utils';
import {
    SHOW_IN_ASSET_SUMMARY_PROPERTY_FILTER_NAME,
    SHOW_IN_COLUMNS_TABLE_PROPERTY_FILTER_NAME,
} from '@src/app/searchV2/utils/constants';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import {
    EntityType,
    Maybe,
    SchemaFieldEntity,
    SearchResult,
    StdDataType,
    StructuredPropertyEntity,
} from '@src/types.generated';
import React, { useState } from 'react';
import { EMPTY_MESSAGES } from '../constants';
import EmptySectionText from '../containers/profile/sidebar/EmptySectionText';
import SectionActionButton from '../containers/profile/sidebar/SectionActionButton';
import { SidebarSection } from '../containers/profile/sidebar/SidebarSection';
import { StyledDivider } from '../tabs/Dataset/Schema/components/SchemaFieldDrawer/components';
import StructuredPropertyValue from '../tabs/Properties/StructuredPropertyValue';
import { PropertyRow } from '../tabs/Properties/types';
import { useHydratedEntityMap } from '../tabs/Properties/useHydratedEntityMap';

interface FieldProperties {
    isSchemaSidebar?: boolean;
    refetch?: () => void;
    fieldEntity?: Maybe<SchemaFieldEntity>;
}

interface Props {
    properties?: FieldProperties;
}

const SidebarStructuredProperties = ({ properties }: Props) => {
    const { entityData, entityType } = useEntityData();
    const me = useUserContext();
    const entityRegistry = useEntityRegistryV2();
    const canEditProps = me.platformPrivileges?.manageStructuredProperties;
    const [isPropModalVisible, setIsPropModalVisible] = useState(false);
    const [selectedProperty, setSelectedProperty] = useState<SearchResult | undefined>();
    const isSchemaSidebar = properties?.isSchemaSidebar || false;

    const inputs = {
        types: [EntityType.StructuredProperty],
        query: '',
        start: 0,
        count: 50,
        searchFlags: { skipCache: true },
        orFilters: [
            {
                and: [
                    getEntityTypesPropertyFilter(entityRegistry, isSchemaSidebar, entityType),
                    getNotHiddenPropertyFilter(),
                    {
                        field: isSchemaSidebar
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
        fetchPolicy: 'cache-first',
    });

    const entityTypeProperties = data?.searchAcrossEntities?.searchResults;

    const allProperties = isSchemaSidebar
        ? properties?.fieldEntity?.structuredProperties
        : entityData?.structuredProperties;

    const selectedPropertyValues = selectedProperty
        ? getPropertyRowFromSearchResult(selectedProperty, allProperties)?.values
        : undefined;

    const uniqueEntityUrnsToHydrate = entityTypeProperties?.flatMap((property) => {
        const propertyRow: PropertyRow | undefined = getPropertyRowFromSearchResult(property, allProperties);
        const values = propertyRow?.values;
        return values?.map((value) => value?.entity?.urn);
    });

    const hydratedEntityMap = useHydratedEntityMap(uniqueEntityUrnsToHydrate);

    return (
        <>
            {entityTypeProperties?.map((property) => {
                const propertyRow: PropertyRow | undefined = getPropertyRowFromSearchResult(property, allProperties);
                const isRichText = propertyRow?.dataType?.info?.type === StdDataType.RichText;
                const values = propertyRow?.values;
                const propertyName = getDisplayName(property.entity as StructuredPropertyEntity);

                return (
                    <>
                        <SidebarSection
                            title={propertyName}
                            key={property.entity.urn}
                            content={
                                <>
                                    {values ? (
                                        <>
                                            {values.map((val) => (
                                                <StructuredPropertyValue
                                                    value={val}
                                                    isRichText={isRichText}
                                                    hydratedEntityMap={hydratedEntityMap}
                                                />
                                            ))}
                                        </>
                                    ) : (
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
                                        actionPrivilege={canEditProps}
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
