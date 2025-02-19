import { Entity } from '@src/types.generated';
import React from 'react';
import styled from 'styled-components';
import { StdDataType } from '../../../../../types.generated';
import { TabRenderType } from '../../types';
import StructuredPropertyValue from './StructuredPropertyValue';
import { PropertyRow } from './types';

interface Props {
    propertyRow: PropertyRow;
    filterText?: string;
    hydratedEntityMap?: Record<string, Entity>;
    renderType: TabRenderType;
}

const ValuesContainerFlex = styled.div<{ renderType: TabRenderType }>`
    display: flex;
    flex-direction: ${(props) => (props.renderType === TabRenderType.COMPACT ? 'column' : 'row')};
    gap: 5px;
    justify-content: flex-start;
    align-items: flex-start; /* Ensure items are aligned at the start */
    flex-wrap: wrap;
`;

const ValueContainer = styled.div`
    flex: 0 1 auto;
`;

export default function ValuesColumn({ propertyRow, filterText, hydratedEntityMap, renderType }: Props) {
    const { values } = propertyRow;
    const isRichText = propertyRow.dataType?.info?.type === StdDataType.RichText;

    return (
        <ValuesContainerFlex renderType={renderType}>
            {values ? (
                values.map((v) => (
                    <ValueContainer>
                        <StructuredPropertyValue
                            value={v}
                            isRichText={isRichText}
                            filterText={filterText}
                            hydratedEntityMap={hydratedEntityMap}
                        />
                    </ValueContainer>
                ))
            ) : (
                <span />
            )}
        </ValuesContainerFlex>
    );
}
