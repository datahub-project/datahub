import React from 'react';
import styled from 'styled-components';

import StructuredPropertyValue from '@app/entityV2/shared/tabs/Properties/StructuredPropertyValue';
import { PropertyRow } from '@app/entityV2/shared/tabs/Properties/types';
import { TabRenderType } from '@app/entityV2/shared/types';
import { Entity } from '@src/types.generated';

import { StdDataType } from '@types';

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
    width: 100%;
`;

const ValueContainer = styled.div`
    width: 100%;
    max-width: 100%;
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
