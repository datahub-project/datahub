import { LoadingOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { Dropdown } from 'antd';
import React, { useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import EditStructuredPropertyModal from '@app/entity/shared/tabs/Properties/Edit/EditStructuredPropertyModal';
import { Icon, Input as InputComponent, Text, colors } from '@src/alchemy-components';
import { useUserContext } from '@src/app/context/useUserContext';
import { getStructuredPropertiesSearchInputs } from '@src/app/govern/structuredProperties/utils';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { PageRoutes } from '@src/conf/Global';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { Maybe, StructuredProperties, StructuredPropertyEntity } from '@src/types.generated';

const AddButton = styled.div<{ isV1Drawer?: boolean }>`
    border-radius: 200px;
    background-color: ${(props) => props.theme.styles['primary-color']};
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
    const me = useUserContext();
    const entityRegistry = useEntityRegistry();
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);

    // Execute search
    const { data, loading } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: getStructuredPropertiesSearchInputs(entityRegistry, entityType, fieldUrn, searchQuery),
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
                menu={{ items: properties }}
                dropdownRender={(menuNode) => (
                    <DropdownContainer data-testid="add-structured-property-dropdown">
                        <SearchContainer>
                            <InputComponent
                                label=""
                                placeholder="Search..."
                                value={searchQuery}
                                setValue={setSearchQuery}
                                inputTestId="search-input"
                            />
                        </SearchContainer>
                        {loading ? (
                            <LoadingContainer>
                                <LoadingOutlined />
                                <Text size="sm">Loading...</Text>
                            </LoadingContainer>
                        ) : (
                            <>
                                {properties?.length === 0 && (
                                    <EmptyContainer>
                                        <Text color="gray" weight="medium">
                                            No results found
                                        </Text>
                                        <Text size="sm" color="gray">
                                            {noDataText}
                                        </Text>
                                    </EmptyContainer>
                                )}
                                <OptionsContainer data-testid="options-container">{menuNode}</OptionsContainer>
                            </>
                        )}
                    </DropdownContainer>
                )}
            >
                <Tooltip title="Add property" placement="left" showArrow={false}>
                    <AddButton isV1Drawer={isV1Drawer} data-testid="add-structured-prop-button">
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
