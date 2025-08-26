import React, { useState } from 'react';
import styled from 'styled-components';

import StructuredPropertyValue from '@app/entityV2/shared/tabs/Properties/StructuredPropertyValue';
import { PropertyRow } from '@app/entityV2/shared/tabs/Properties/types';
import { TabRenderType } from '@app/entityV2/shared/types';
import ProposalModal from '@app/shared/tags/ProposalModal';
import { ActionRequest, Entity } from '@src/types.generated';

import { StdDataType } from '@types';

interface Props {
    propertyRow: PropertyRow & { request?: ActionRequest };
    filterText?: string;
    hydratedEntityMap?: Record<string, Entity>;
    renderType: TabRenderType;
    isProposed?: boolean;
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

export default function ValuesColumn({ propertyRow, filterText, hydratedEntityMap, renderType, isProposed }: Props) {
    const { values } = propertyRow;
    const isRichText = propertyRow.dataType?.info?.type === StdDataType.RichText;

    const [selectedActionRequest, setSelectedActionRequest] = useState<ActionRequest | undefined | null>(null);

    return (
        <>
            <ValuesContainerFlex renderType={renderType}>
                {values ? (
                    values.map((v) => (
                        <ValueContainer
                            onClick={() => {
                                setSelectedActionRequest(propertyRow.request);
                            }}
                        >
                            <StructuredPropertyValue
                                value={v}
                                isRichText={isRichText}
                                filterText={filterText}
                                hydratedEntityMap={hydratedEntityMap}
                                isProposed={isProposed}
                            />
                        </ValueContainer>
                    ))
                ) : (
                    <span />
                )}
            </ValuesContainerFlex>
            {selectedActionRequest && (
                <ProposalModal
                    actionRequest={selectedActionRequest}
                    selectedActionRequest={selectedActionRequest}
                    setSelectedActionRequest={setSelectedActionRequest}
                />
            )}
        </>
    );
}
