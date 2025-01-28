import { LoadingOutlined } from '@ant-design/icons';
import { colors, Icon, Input as InputComponent, Text } from '@src/alchemy-components';
import { useUserContext } from '@src/app/context/useUserContext';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { getEntityTypesPropertyFilter, getNotHiddenPropertyFilter } from '@src/app/govern/structuredProperties/utils';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { useIsThemeV2 } from '@src/app/useIsThemeV2';
import { PageRoutes } from '@src/conf/Global';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { Dropdown } from 'antd';
import { Tooltip } from '@components';
import { EntityType, Maybe, StructuredProperties, StructuredPropertyEntity } from '@src/types.generated';
import React, { useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';
import { useEntityData } from '../../EntityContext';
import EditStructuredPropertyModal from './Edit/EditStructuredPropertyModal';

const AddButton = styled.div<{ isThemeV2: boolean; isV1Drawer?: boolean }>`
    border-radius: 200px;
    background-color: ${(props) => (props.isThemeV2 ? colors.violet[500] : REDESIGN_COLORS.LINK_HOVER_BLUE)};
    width: ${(props) => (props.isV1Drawer ? '24px' : '32px')};
    height: ${(props) => (props.isV1Drawer ? '24px' : '32px')};
    display: flex;
    align-items: center;
    justify-content: center;

    :hover {
        cursor: pointer;
    }
`;

const DropdownContainer = styled.div`
    border-radius: 12px;
    box-shadow: 0px 0px 14px 0px rgba(0, 0, 0, 0.15);
    background-color: ${colors.white};
    padding-bottom: 8px;
    width: 300px;
`;

const SearchContainer = styled.div`
    padding: 8px;
`;

const OptionsContainer = styled.div`
    max-height: 200px;
    overflow-y: auto;
    font-size: 14px;
`;

const Option = styled.div``;

const LoadingContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
    align-items: center;
    justify-content: center;
    height: 100px;
`;

const EmptyContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
    align-items: center;
    justify-content: center;
    min-height: 50px;
    padding: 16px;
    text-align: center;
`;

interface Props {
    fieldUrn?: string;
    refetch?: () => void;
    fieldProperties?: Maybe<StructuredProperties>;
    isV1Drawer?: boolean;
}

const AddPropertyButton = ({ fieldUrn, refetch, fieldProperties, isV1Drawer }: Props) => {
    const [searchQuery, setSearchQuery] = useState('');
    const { entityData, entityType } = useEntityData();
    const isThemeV2 = useIsThemeV2();
    const me = useUserContext();
    const entityRegistry = useEntityRegistry();
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);

    const inputs = {
        types: [EntityType.StructuredProperty],
        query: '',
        start: 0,
        count: 100,
        searchFlags: { skipCache: true },
        orFilters: [
            {
                and: [
                    getEntityTypesPropertyFilter(entityRegistry, !!fieldUrn, entityType),
                    getNotHiddenPropertyFilter(),
                ],
            },
        ],
    };

    // Execute search
    const { data, loading } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: inputs,
        },
        fetchPolicy: 'cache-first',
    });

    const [selectedProperty, setSelectedProperty] = useState<StructuredPropertyEntity | undefined>(
        data?.searchAcrossEntities?.searchResults?.[0]?.entity as StructuredPropertyEntity | undefined,
    );

    const handleOptionClick = (property: StructuredPropertyEntity) => {
        setSelectedProperty(property);
        setIsEditModalVisible(true);
    };

    const entityPropertiesUrns = entityData?.structuredProperties?.properties?.map(
        (prop) => prop.structuredProperty.urn,
    );
    const fieldPropertiesUrns = fieldProperties?.properties?.map((prop) => prop.structuredProperty.urn);

    // filter out the existing properties when displaying in the list of add button
    const properties = useMemo(
        () =>
            data?.searchAcrossEntities?.searchResults
                .filter((result) =>
                    fieldUrn
                        ? !fieldPropertiesUrns?.includes(result.entity.urn)
                        : !entityPropertiesUrns?.includes(result.entity.urn),
                )
                .map((prop) => {
                    const entity = prop.entity as StructuredPropertyEntity;
                    const name = entityRegistry.getDisplayName(entity.type, entity);
                    return {
                        label: (
                            <Option key={entity.urn} onClick={() => handleOptionClick(entity)}>
                                <Text weight="semiBold" color="gray">
                                    {name}
                                </Text>
                            </Option>
                        ),
                        key: entity.urn,
                        name: name || entity.urn,
                    };
                }),
        [data, fieldUrn, fieldPropertiesUrns, entityPropertiesUrns, entityRegistry],
    );

    const canEditProperties =
        entityData?.parent?.privileges?.canEditProperties || entityData?.privileges?.canEditProperties;

    if (!canEditProperties) return null;

    // Filter items based on search query
    const filteredItems = properties?.filter((prop) => prop.name?.toLowerCase().includes(searchQuery.toLowerCase()));

    const noDataText =
        properties?.length === 0 ? (
            <>
                It looks like there are no structured properties for this asset type.
                {me.platformPrivileges?.manageStructuredProperties && (
                    <span>
                        {' '}
                        <Link to={PageRoutes.STRUCTURED_PROPERTIES}>Manage custom properties</Link>
                    </span>
                )}
            </>
        ) : null;

    return (
        <>
            <Dropdown
                trigger={['click']}
                menu={{ items: filteredItems }}
                dropdownRender={(menuNode) => (
                    <DropdownContainer>
                        <SearchContainer>
                            <InputComponent
                                label=""
                                placeholder="Search..."
                                value={searchQuery}
                                onChange={(e) => setSearchQuery(e.target.value)}
                            />
                        </SearchContainer>
                        {loading ? (
                            <LoadingContainer>
                                <LoadingOutlined />
                                <Text size="sm">Loading...</Text>
                            </LoadingContainer>
                        ) : (
                            <>
                                {filteredItems?.length === 0 && (
                                    <EmptyContainer>
                                        <Text color="gray" weight="medium">
                                            No results found
                                        </Text>
                                        <Text size="sm" color="gray">
                                            {noDataText}
                                        </Text>
                                    </EmptyContainer>
                                )}
                                <OptionsContainer>{menuNode}</OptionsContainer>
                            </>
                        )}
                    </DropdownContainer>
                )}
            >
                <Tooltip title="Add property" placement="left" showArrow={false}>
                    <AddButton isThemeV2={isThemeV2} isV1Drawer={isV1Drawer} data-testid="add-structured-prop-button">
                        <Icon icon="Add" size={isV1Drawer ? 'lg' : '2xl'} color="white" />
                    </AddButton>
                </Tooltip>
            </Dropdown>
            {selectedProperty && (
                <EditStructuredPropertyModal
                    isOpen={isEditModalVisible}
                    closeModal={() => setIsEditModalVisible(false)}
                    structuredProperty={selectedProperty}
                    associatedUrn={fieldUrn} // pass in fieldUrn to use otherwise we will use mutation urn for siblings
                    refetch={refetch}
                    isAddMode
                />
            )}
        </>
    );
};

export default AddPropertyButton;
