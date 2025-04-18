import { Text } from '@components';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { PropertyCardinality, SearchResult, StructuredPropertyEntity } from '@src/types.generated';
import React from 'react';
import {
    DescriptionContainer,
    DrawerHeader,
    ItemsList,
    RowContainer,
    StyledDrawer,
    StyledIcon,
    StyledLabel,
    VerticalDivider,
    ViewDivider,
    ViewFieldsContainer,
} from './styledComponents';
import { getDisplayName, getValueTypeLabel } from './utils';
import ViewAdvancedOptions from './ViewAdvancedOptions';
import ViewDisplayPreferences from './ViewDisplayPreferences';

interface Props {
    isViewDrawerOpen: boolean;
    setIsViewDrawerOpen: React.Dispatch<React.SetStateAction<boolean>>;
    selectedProperty: SearchResult;
    setSelectedProperty: React.Dispatch<React.SetStateAction<SearchResult | undefined>>;
}

const ViewStructuredPropsDrawer = ({
    isViewDrawerOpen,
    setIsViewDrawerOpen,
    selectedProperty,
    setSelectedProperty,
}: Props) => {
    const entityRegistry = useEntityRegistry();

    const handleClose = () => {
        setIsViewDrawerOpen(false);
        setSelectedProperty(undefined);
    };

    const selectedPropEntity = selectedProperty && (selectedProperty?.entity as StructuredPropertyEntity);

    const allowedValues = selectedPropEntity?.definition?.allowedValues;

    const allowedTypes = selectedPropEntity?.definition?.typeQualifier?.allowedTypes;

    const propType = getValueTypeLabel(
        selectedPropEntity.definition.valueType.urn,
        selectedPropEntity.definition.cardinality || PropertyCardinality.Single,
    );

    return (
        <StyledDrawer
            open={isViewDrawerOpen}
            closable={false}
            width={480}
            title={
                <>
                    {selectedProperty && (
                        <DrawerHeader>
                            <Text color="gray" weight="bold" size="2xl">
                                {getDisplayName(selectedProperty?.entity as StructuredPropertyEntity)}
                            </Text>

                            <StyledIcon icon="Close" color="gray" onClick={handleClose} />
                        </DrawerHeader>
                    )}
                </>
            }
            footer={null}
            destroyOnClose
        >
            <ViewFieldsContainer>
                {selectedPropEntity.definition.description && (
                    <DescriptionContainer>
                        <Text weight="bold" color="gray" size="lg">
                            Description
                        </Text>
                        <Text color="gray"> {selectedPropEntity.definition.description}</Text>
                        <ViewDivider />
                    </DescriptionContainer>
                )}
                <RowContainer>
                    <StyledLabel>Property Type</StyledLabel>
                    <Text color="gray"> {propType}</Text>
                </RowContainer>
                {allowedTypes && allowedTypes.length > 0 && (
                    <RowContainer>
                        <StyledLabel>Allowed Entity Types</StyledLabel>
                        <ItemsList>
                            {allowedTypes.map((type, index) => {
                                return (
                                    <>
                                        <Text color="gray">{entityRegistry.getEntityName(type.info.type)}</Text>
                                        {index < allowedTypes.length - 1 && <VerticalDivider type="vertical" />}
                                    </>
                                );
                            })}
                        </ItemsList>
                    </RowContainer>
                )}
                {allowedValues && allowedValues.length > 0 && (
                    <RowContainer>
                        <StyledLabel>Allowed Values</StyledLabel>
                        <ItemsList>
                            {allowedValues?.map((val, index) => {
                                return (
                                    <>
                                        <Text color="gray">
                                            {(val.value as any).stringValue || (val.value as any).numberValue}
                                        </Text>
                                        {index < allowedValues?.length - 1 && <VerticalDivider type="vertical" />}
                                    </>
                                );
                            })}
                        </ItemsList>
                    </RowContainer>
                )}

                <RowContainer>
                    <StyledLabel>Applies To</StyledLabel>
                    <ItemsList>
                        {selectedPropEntity.definition.entityTypes?.map((type, index) => {
                            return (
                                <>
                                    <Text color="gray">{entityRegistry.getEntityName(type.info.type)}</Text>
                                    {index < selectedPropEntity.definition.entityTypes?.length - 1 && (
                                        <VerticalDivider type="vertical" />
                                    )}
                                </>
                            );
                        })}
                    </ItemsList>
                </RowContainer>
            </ViewFieldsContainer>
            <ViewDisplayPreferences propEntity={selectedPropEntity} />
            <ViewAdvancedOptions propEntity={selectedPropEntity} />
        </StyledDrawer>
    );
};

export default ViewStructuredPropsDrawer;
