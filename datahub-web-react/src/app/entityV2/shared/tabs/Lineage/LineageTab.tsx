import LineageGraph from '@app/lineageV2/LineageGraph';
import React, { useContext } from 'react';
import styled from 'styled-components';
import { useLineageV2 } from '../../../../lineageV2/useLineageV2';
import { LineageDirection } from '../../../../../types.generated';
import { useEntityData } from '../../../../entity/shared/EntityContext';
import { TabRenderType } from '../../types';
import { CompactLineageTab } from './CompactLineageTab';
import LineageExplorer from '../../../../lineage/LineageExplorer';
import { LineageColumnView } from './LineageColumnView';
import TabFullsizedContext from '../../../../shared/TabFullsizedContext';
import { useLineageViewState } from './hooks';

const LINEAGE_SWITCH_WIDTH = 90;

const LineageTabWrapper = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
`;

const LineageSwitchWrapper = styled.div`
    border: 1px solid #5d09c9;
    border-radius: 4.5px;
    display: flex;
    margin: 13px 11px;
    width: ${LINEAGE_SWITCH_WIDTH * 2}px;
`;

const LineageViewSwitch = styled.div<{ selected: boolean }>`
    background: ${({ selected }) => (selected ? '#5d09c9' : '#fff')};
    border-radius: 3px;
    color: ${({ selected }) => (selected ? '#fff' : '#5d09c9')};
    cursor: pointer;
    display: flex;
    font-size: 10px;
    justify-content: center;
    line-height: 24px;
    height: 24px;
    width: ${LINEAGE_SWITCH_WIDTH}px;
`;

const VisualizationWrapper = styled.div`
    display: flex;
    height: 100%;
`;

const LineageTabHeader = styled.div`
    display: flex;
    justify-content: space-between;
`;

interface Props {
    properties?: { defaultDirection: LineageDirection };
    renderType: TabRenderType;
}

export function LineageTab({ properties, renderType }: Props) {
    const defaultDirection = properties?.defaultDirection || LineageDirection.Downstream;

    if (renderType === TabRenderType.COMPACT) {
        return <CompactLineageTab defaultDirection={defaultDirection} />;
    }
    return <WideLineageTab defaultDirection={defaultDirection} />;
}

function WideLineageTab({ defaultDirection }: { defaultDirection: LineageDirection }) {
    const { isTabFullsize } = useContext(TabFullsizedContext);
    const { urn, entityType } = useEntityData();
    const isLineageV2 = useLineageV2();
    const { isVisualizeView, setVisualizeView } = useLineageViewState();

    return (
        <LineageTabWrapper>
            {!isTabFullsize && (
                <LineageTabHeader>
                    <LineageSwitchWrapper>
                        <LineageViewSwitch selected={isVisualizeView} onClick={() => setVisualizeView(true)}>
                            Explorer
                        </LineageViewSwitch>
                        <LineageViewSwitch selected={!isVisualizeView} onClick={() => setVisualizeView(false)}>
                            Impact Analysis
                        </LineageViewSwitch>
                    </LineageSwitchWrapper>
                </LineageTabHeader>
            )}
            {!isVisualizeView && <LineageColumnView defaultDirection={defaultDirection} />}
            {isVisualizeView && !isLineageV2 && <LineageExplorer urn={urn} type={entityType} />}
            {isVisualizeView && isLineageV2 && (
                <VisualizationWrapper>
                    <LineageGraph />
                </VisualizationWrapper>
            )}
        </LineageTabWrapper>
    );
}
