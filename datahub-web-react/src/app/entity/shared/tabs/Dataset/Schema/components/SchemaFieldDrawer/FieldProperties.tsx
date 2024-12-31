import { useEntityData } from '@src/app/entity/shared/EntityContext';
import React from 'react';
import styled from 'styled-components';
import { SchemaField, SearchResult, StdDataType } from '../../../../../../../../types.generated';
import AddPropertyButton from '../../../../Properties/AddPropertyButton';
import { EditColumn } from '../../../../Properties/Edit/EditColumn';
import StructuredPropertyValue from '../../../../Properties/StructuredPropertyValue';
import { mapStructuredPropertyValues } from '../../../../Properties/useStructuredProperties';
import { useGetEntityWithSchema } from '../../useGetEntitySchema';
import { StyledDivider } from './components';

export const PropertyTitle = styled.div`
    font-size: 14px;
    font-weight: 700;
    margin-bottom: 4px;
`;

export const PropertyWrapper = styled.div`
    margin-bottom: 12px;
    display: flex;
    justify-content: space-between;
`;

export const PropertiesWrapper = styled.div`
    padding-left: 16px;
`;

export const StyledList = styled.ul`
    padding-left: 24px;
`;

export const Header = styled.div`
    font-size: 16px;
    font-weight: 600;
    margin-bottom: 16px;
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

interface Props {
    expandedField: SchemaField;
    schemaColumnProperties?: SearchResult[];
}

export default function FieldProperties({ expandedField, schemaColumnProperties }: Props) {
    const { schemaFieldEntity } = expandedField;
    const { refetch } = useGetEntityWithSchema(true);
    const { entityData } = useEntityData();
    const properties =
        schemaFieldEntity?.structuredProperties?.properties?.filter(
            (prop) =>
                prop.structuredProperty.exists &&
                !prop.structuredProperty.settings?.isHidden &&
                !schemaColumnProperties?.find((p) => p.entity.urn === prop.structuredProperty.urn),
        ) || [];

    const canEditProperties =
        entityData?.parent?.privileges?.canEditProperties || entityData?.privileges?.canEditProperties;

    if (!schemaFieldEntity) return null;

    return (
        <>
            <Header>
                Properties
                <AddPropertyButton
                    fieldUrn={schemaFieldEntity?.urn}
                    fieldProperties={schemaFieldEntity.structuredProperties}
                    refetch={refetch}
                    isV1Drawer
                />
            </Header>
            <PropertiesWrapper>
                {properties.map((structuredProp) => {
                    const isRichText =
                        structuredProp.structuredProperty.definition.valueType?.info?.type === StdDataType.RichText;
                    const valuesData = mapStructuredPropertyValues(structuredProp);
                    const hasMultipleValues = valuesData.length > 1;

                    return (
                        <PropertyWrapper key={structuredProp.structuredProperty.urn}>
                            <div>
                                <PropertyTitle>
                                    {structuredProp.structuredProperty.definition.displayName}
                                </PropertyTitle>
                                {hasMultipleValues ? (
                                    <StyledList>
                                        {valuesData.map((value) => (
                                            <li>
                                                <StructuredPropertyValue value={value} isRichText={isRichText} />
                                            </li>
                                        ))}
                                    </StyledList>
                                ) : (
                                    <>
                                        {valuesData.map((value) => (
                                            <StructuredPropertyValue value={value} isRichText={isRichText} />
                                        ))}
                                    </>
                                )}
                            </div>
                            {canEditProperties && (
                                <EditColumn
                                    structuredProperty={structuredProp.structuredProperty}
                                    associatedUrn={structuredProp.associatedUrn}
                                    values={valuesData.map((v) => v.value) || []}
                                    refetch={refetch}
                                />
                            )}
                        </PropertyWrapper>
                    );
                })}
            </PropertiesWrapper>
            <StyledDivider />
        </>
    );
}
