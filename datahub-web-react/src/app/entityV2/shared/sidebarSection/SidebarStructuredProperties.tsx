import AddRoundedIcon from '@mui/icons-material/AddRounded';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import { useUserContext } from '@src/app/context/useUserContext';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import EditStructuredPropertyModal from '@src/app/entity/shared/tabs/Properties/Edit/EditStructuredPropertyModal';
import { getDisplayName, getEntityTypeUrn } from '@src/app/govern/structuredProperties/utils';
import { ENTITY_TYPES_FILTER_NAME } from '@src/app/searchV2/utils/constants';
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
import { mapStructuredPropertyToPropertyRow } from '../tabs/Properties/useStructuredProperties';

interface Properties {
    isSchemaSidebar?: boolean;
    refetch?: () => void;
    fieldEntity?: Maybe<SchemaFieldEntity>;
}

interface Props {
    properties?: Properties;
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
                    {
                        field: ENTITY_TYPES_FILTER_NAME,
                        values: [
                            getEntityTypeUrn(entityRegistry, isSchemaSidebar ? EntityType.SchemaField : entityType),
                        ],
                    },
                    {
                        field: 'isHidden',
                        values: ['true'],
                        negated: true,
                    },
                    {
                        field: isSchemaSidebar ? 'showInColumnsTable' : 'showInAssetSummary',
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

    const getPropertyRowFromSearchResult = (property: SearchResult) => {
        const allProperties = isSchemaSidebar
            ? properties?.fieldEntity?.structuredProperties
            : entityData?.structuredProperties;
        const entityProp = allProperties?.properties?.filter(
            (prop) => prop.structuredProperty.urn === property.entity.urn,
        )[0];
        return entityProp ? mapStructuredPropertyToPropertyRow(entityProp) : undefined;
    };

    return (
        <>
            {entityTypeProperties?.map((property) => {
                const propertyRow: PropertyRow | undefined = getPropertyRowFromSearchResult(property);
                const isRichText = propertyRow?.dataType?.info.type === StdDataType.RichText;
                const values = propertyRow?.values;

                return (
                    <>
                        <SidebarSection
                            title={getDisplayName(property.entity as StructuredPropertyEntity)}
                            key={property.entity.urn}
                            content={
                                <>
                                    {values ? (
                                        <>
                                            {values.map((val) => (
                                                <StructuredPropertyValue value={val} isRichText={isRichText} />
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
                    isAddMode={!getPropertyRowFromSearchResult(selectedProperty)?.values}
                    values={getPropertyRowFromSearchResult(selectedProperty)?.values?.map((val) => val.value)}
                    refetch={isSchemaSidebar ? properties?.refetch : undefined}
                    associatedUrn={isSchemaSidebar ? properties?.fieldEntity?.urn : undefined}
                />
            )}
        </>
    );
};

export default SidebarStructuredProperties;
