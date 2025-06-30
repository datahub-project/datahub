import { colors } from '@components';
import React, { useContext } from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { CompactLineageTab } from '@app/entityV2/shared/tabs/Lineage/CompactLineageTab';
import { LineageColumnView } from '@app/entityV2/shared/tabs/Lineage/LineageColumnView';
import { useLineageViewState } from '@app/entityV2/shared/tabs/Lineage/hooks';
import { TabRenderType } from '@app/entityV2/shared/types';
import LineageExplorer from '@app/lineage/LineageExplorer';
import LineageGraph from '@app/lineageV2/LineageGraph';
import { useLineageV2 } from '@app/lineageV2/useLineageV2';
import TabFullsizedContext from '@app/shared/TabFullsizedContext';
import { getColor } from '@src/alchemy-components/theme/utils';

import { LineageDirection } from '@types';

const LINEAGE_SWITCH_WIDTH = 90;

const LineageTabWrapper = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
`;

const LineageSwitchWrapper = styled.div`
    border: 1px solid ${colors.violet[600]};
    border-radius: 4.5px;
    display: flex;
    margin: 13px 11px;
    width: ${LINEAGE_SWITCH_WIDTH * 2}px;
`;

const LineageViewSwitch = styled.div<{ selected: boolean }>`
    background: ${({ selected, theme }) => (selected ? `${getColor('primary', 600, theme)}` : '#fff')};
    border-radius: 3px;
    color: ${({ selected, theme }) => (selected ? '#fff' : `${getColor('primary', 600, theme)}`)};
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
    const { isVisualizeView, setVisualizeView, setVisualizeViewInEditMode } = useLineageViewState();

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
            {!isVisualizeView && (
                <LineageColumnView
                    defaultDirection={defaultDirection}
                    setVisualizeViewInEditMode={setVisualizeViewInEditMode}
                />
            )}
            {isVisualizeView && !isLineageV2 && <LineageExplorer urn={urn} type={entityType} />}
            {isVisualizeView && isLineageV2 && (
                <VisualizationWrapper>
                    <LineageGraph />
                </VisualizationWrapper>
            )}
        </LineageTabWrapper>
    );
}
