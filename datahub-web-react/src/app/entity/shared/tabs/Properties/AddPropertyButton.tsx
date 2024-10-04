import { LoadingOutlined } from '@ant-design/icons';
import { colors, Icon, Input as InputComponent, Text } from '@src/alchemy-components';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { useIsThemeV2 } from '@src/app/useIsThemeV2';
import { useGetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import { EntityType, StructuredPropertyEntity } from '@src/types.generated';
import { Dropdown, Tooltip } from 'antd';
import React, { useMemo, useState } from 'react';
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
    padding: 0 8px 8px 8px;
`;

const SearchContainer = styled.div`
    padding-bottom: 8px;
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

interface Props {
    fieldUrn?: string;
    refetch?: () => void;
    isV1Drawer?: boolean;
}

const AddPropertyButton = ({ fieldUrn, refetch, isV1Drawer }: Props) => {
    const [searchQuery, setSearchQuery] = useState('');
    const { entityData, entityType } = useEntityData();
    const isThemeV2 = useIsThemeV2();
    const [isEditModalVisible, setIsEditModalVisible] = useState(false);

    const inputs = {
        types: [EntityType.StructuredProperty],
        query: '',
        start: 0,
        count: 100,
        searchFlags: { skipCache: true },
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

    // only display structured props that can be applied to the asset type
    const properties = useMemo(
        () =>
            data?.searchAcrossEntities?.searchResults
                .filter((result) =>
                    (result.entity as StructuredPropertyEntity).definition.entityTypes.find((t) => {
                        if (fieldUrn) return t.info.type === EntityType.SchemaField;
                        return t.info.type === entityType;
                    }),
                )
                .map((prop) => {
                    const entity = prop.entity as StructuredPropertyEntity;
                    return {
                        label: (
                            <Option key={entity.urn} onClick={() => handleOptionClick(entity)}>
                                <Text weight="semiBold" color="gray">
                                    {entity.definition?.displayName}
                                </Text>
                            </Option>
                        ),
                        key: entity.urn,
                        name: entity.definition?.displayName || entity.urn,
                    };
                }),
        [data, entityType, fieldUrn],
    );

    const canEditProperties =
        entityData?.parent?.privileges?.canEditProperties || entityData?.privileges?.canEditProperties;

    if (!canEditProperties) return null;

    // Filter items based on search query
    const filteredItems = properties?.filter((prop) => prop.name?.toLowerCase().includes(searchQuery.toLowerCase()));
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
                            <OptionsContainer>{menuNode}</OptionsContainer>
                        )}
                    </DropdownContainer>
                )}
            >
                <Tooltip title="Add structured property" placement="left" showArrow={false}>
                    <AddButton isThemeV2={isThemeV2} isV1Drawer={isV1Drawer}>
                        <Icon icon="Add" size={isV1Drawer ? 'lg' : '2xl'} color="white" />
                    </AddButton>
                </Tooltip>
            </Dropdown>
            {selectedProperty && (
                <EditStructuredPropertyModal
                    isOpen={isEditModalVisible}
                    closeModal={() => setIsEditModalVisible(false)}
                    structuredProperty={selectedProperty}
                    associatedUrn={fieldUrn || entityData?.urn}
                    refetch={refetch}
                    isAddMode
                />
            )}
        </>
    );
};

export default AddPropertyButton;
