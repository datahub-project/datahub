import { Tooltip } from 'antd';
import React from 'react';
import styled from 'styled-components';

import StructuredPropertyValue from '@src/app/entityV2/shared/tabs/Properties/StructuredPropertyValue';
import { useGetProposedProperties } from '@src/app/entityV2/shared/tabs/Properties/useGetProposedProperties';
import { useHydratedEntityMap } from '@src/app/entityV2/shared/tabs/Properties/useHydratedEntityMap';
import { mapStructuredPropertyToPropertyRow } from '@src/app/entityV2/shared/tabs/Properties/useStructuredProperties';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { SchemaFieldEntity, SearchResult, StdDataType } from '@src/types.generated';

const ValuesContainer = styled.span`
    max-width: 150px;
    display: flex;
`;

const MoreIndicator = styled.span`
    float: right;
`;

const Container = styled.span`
    max-width: 100%;
`;

const NO_OF_VALUES_TO_SHOW_IN_TABLE = 2;

interface Props {
    schemaFieldEntity: SchemaFieldEntity | undefined;
    propColumn: SearchResult | undefined;
}

const StructuredPropValues = ({ schemaFieldEntity, propColumn }: Props) => {
    const entityRegistry = useEntityRegistry();

    const { proposedProperties, proposedValues } = useGetProposedProperties({
        fieldPath: schemaFieldEntity?.fieldPath,
        propertyUrn: propColumn?.entity?.urn,
    });

    const property = schemaFieldEntity?.structuredProperties?.properties?.find(
        (prop) => prop.structuredProperty.urn === propColumn?.entity?.urn,
    );
    const propRow = property ? mapStructuredPropertyToPropertyRow(property) : undefined;
    const propValues = propRow?.values;
    const combinedValues = [...(propValues || []), ...proposedValues];
    const isRichText =
        propRow?.dataType?.info?.type === StdDataType.RichText ||
        proposedProperties[0]?.structuredProperty?.definition?.valueType?.info?.type === StdDataType.RichText;

    const hasMoreValues = combinedValues && combinedValues.length > NO_OF_VALUES_TO_SHOW_IN_TABLE;
    const displayedValues = hasMoreValues
        ? propValues?.slice(0, NO_OF_VALUES_TO_SHOW_IN_TABLE - 1) || []
        : propValues || [];
    const remainingSlots = NO_OF_VALUES_TO_SHOW_IN_TABLE - displayedValues.length;
    const displayedProposedValues = proposedValues.slice(0, remainingSlots);

    const hydratedEntityMap = useHydratedEntityMap(combinedValues.map((val) => val.entity?.urn));

    const tooltipContent = combinedValues?.map((value) => {
        const title = value.entity
            ? entityRegistry.getDisplayName(value.entity.type, value.entity)
            : value.value?.toString();
        return <div>{title}</div>;
    });

    return (
        <Container>
            {combinedValues && (
                <>
                    {displayedValues?.map((val) => {
                        return (
                            <ValuesContainer>
                                <StructuredPropertyValue
                                    value={val}
                                    isRichText={isRichText}
                                    truncateText
                                    isFieldColumn
                                    hydratedEntityMap={hydratedEntityMap}
                                />
                            </ValuesContainer>
                        );
                    })}
                    {displayedProposedValues?.map((val) => {
                        return (
                            <ValuesContainer>
                                <StructuredPropertyValue
                                    value={val}
                                    isRichText={isRichText}
                                    truncateText
                                    isFieldColumn
                                    isProposed
                                    hydratedEntityMap={hydratedEntityMap}
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
        </Container>
    );
};

export default StructuredPropValues;
