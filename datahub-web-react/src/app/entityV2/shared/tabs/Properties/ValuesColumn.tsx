import React from 'react';
import styled from 'styled-components';

import StructuredPropertyValueList from '@app/entityV2/shared/tabs/Properties/StructuredPropertyValueList';
import { PropertyRow } from '@app/entityV2/shared/tabs/Properties/types';
import { TabRenderType } from '@app/entityV2/shared/types';

import { StdDataType } from '@types';

/** Table rows are wider than sidebar sections, so a cell shows more values before paging. */
const MAX_VALUES_PER_TABLE_CELL = 100;

interface Props {
    propertyRow: PropertyRow;
    filterText?: string;
    renderType: TabRenderType;
    /** Whose values these are (entity, or entity + schema field), so list state resets per asset. */
    scopeKey: string;
}

const ValuesContainerFlex = styled.div<{ renderType: TabRenderType }>`
    display: flex;
    flex-direction: ${(props) => (props.renderType === TabRenderType.COMPACT ? 'column' : 'row')};
    gap: 5px;
    justify-content: flex-start;
    align-items: flex-start; /* Ensure items are aligned at the start */
    flex-wrap: wrap;
    width: 100%;
`;

const ValueContainer = styled.div`
    width: 100%;
    max-width: 100%;
`;

export default function ValuesColumn({ propertyRow, filterText, renderType, scopeKey }: Props) {
    const { values } = propertyRow;
    const isRichText = propertyRow.dataType?.info?.type === StdDataType.RichText;

    return (
        <>
            <ValuesContainerFlex renderType={renderType}>
                {values ? (
                    <StructuredPropertyValueList
                        key={`${scopeKey}:${propertyRow.qualifiedName}`}
                        propertyRow={propertyRow}
                        isRichText={isRichText}
                        filterText={filterText}
                        maxValuesToShow={MAX_VALUES_PER_TABLE_CELL}
                        renderValue={(value, node) => <ValueContainer key={`${value.value}`}>{node}</ValueContainer>}
                    />
                ) : (
                    <span />
                )}
            </ValuesContainerFlex>
        </>
    );
}
