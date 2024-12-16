import StructuredPropertyValue from '@src/app/entity/shared/tabs/Properties/StructuredPropertyValue';
import { mapStructuredPropertyToPropertyRow } from '@src/app/entity/shared/tabs/Properties/useStructuredProperties';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { SchemaFieldEntity, SearchResult, StdDataType } from '@src/types.generated';
import { Tooltip } from 'antd';
import React from 'react';
import styled from 'styled-components';

const ValuesContainer = styled.span`
    max-width: 120px;
    display: flex;
`;

const MoreIndicator = styled.span`
    float: right;
`;

interface Props {
    schemaFieldEntity: SchemaFieldEntity;
    propColumn: SearchResult | undefined;
}

const StructuredPropValues = ({ schemaFieldEntity, propColumn }: Props) => {
    const entityRegistry = useEntityRegistry();

    const property = schemaFieldEntity.structuredProperties?.properties?.find(
        (prop) => prop.structuredProperty.urn === propColumn?.entity?.urn,
    );
    const propRow = property ? mapStructuredPropertyToPropertyRow(property) : undefined;
    const values = propRow?.values;
    const isRichText = propRow?.dataType?.info?.type === StdDataType.RichText;

    const hasMoreValues = values && values.length > 2;
    const displayedValues = hasMoreValues ? values.slice(0, 1) : values;
    const tooltipContent = values?.map((value) => {
        const title = value.entity
            ? entityRegistry.getDisplayName(value.entity.type, value.entity)
            : value.value?.toString();
        return <div>{title}</div>;
    });

    return (
        <>
            {values && (
                <>
                    {displayedValues?.map((val) => {
                        return (
                            <ValuesContainer>
                                <StructuredPropertyValue
                                    value={val}
                                    isRichText={isRichText}
                                    truncateText
                                    isFieldColumn
                                />
                            </ValuesContainer>
                        );
                    })}
                    {hasMoreValues && (
                        <Tooltip title={tooltipContent} showArrow={false}>
                            <MoreIndicator>...</MoreIndicator>
                        </Tooltip>
                    )}
                </>
            )}
        </>
    );
};

export default StructuredPropValues;
