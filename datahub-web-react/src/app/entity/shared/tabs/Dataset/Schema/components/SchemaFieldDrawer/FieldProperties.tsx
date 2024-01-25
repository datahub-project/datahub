import React from 'react';
import styled from 'styled-components';
import { SchemaField, StdDataType } from '../../../../../../../../types.generated';
import { SectionHeader, StyledDivider } from './components';
import { mapStructuredPropertyValues } from '../../../../Properties/useStructuredProperties';
import StructuredPropertyValue from '../../../../Properties/StructuredPropertyValue';

const PropertyTitle = styled.div`
    font-size: 14px;
    font-weight: 700;
    margin-bottom: 4px;
`;

const PropertyWrapper = styled.div`
    margin-bottom: 12px;
`;

const PropertiesWrapper = styled.div`
    padding-left: 16px;
`;

const StyledList = styled.ul`
    padding-left: 24px;
`;

interface Props {
    expandedField: SchemaField;
}

export default function FieldProperties({ expandedField }: Props) {
    const { schemaFieldEntity } = expandedField;

    if (!schemaFieldEntity?.structuredProperties?.properties?.length) return null;

    return (
        <>
            <SectionHeader>Properties</SectionHeader>
            <PropertiesWrapper>
                {schemaFieldEntity.structuredProperties.properties.map((structuredProp) => {
                    const isRichText =
                        structuredProp.structuredProperty.definition.valueType?.info.type === StdDataType.RichText;
                    const valuesData = mapStructuredPropertyValues(structuredProp);
                    const hasMultipleValues = valuesData.length > 1;

                    return (
                        <PropertyWrapper>
                            <PropertyTitle>{structuredProp.structuredProperty.definition.displayName}</PropertyTitle>
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
                        </PropertyWrapper>
                    );
                })}
            </PropertiesWrapper>
            <StyledDivider />
        </>
    );
}
