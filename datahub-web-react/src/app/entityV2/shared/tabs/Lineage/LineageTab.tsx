import React, { useContext, useEffect, useState } from 'react';
import styled from 'styled-components';

import { useLineageV2 } from '../../../../lineageV2/useLineageV2';
import { EntityType, LineageDirection } from '../../../../../types.generated';
import { useEntityData } from '../../EntityContext';
import { TabRenderType } from '../../types';
import { CompactLineageTab } from './CompactLineageTab';
import LineageExplorer from '../../../../lineage/LineageExplorer';
import LineageExplorerV2 from '../../../../lineageV2/LineageExplorer';
import { LineageColumnView } from './LineageColumnView';
import TabFullsizedContext from '../../../../shared/TabFullsizedContext';

const LINEAGE_SWITCH_WIDTH = 90;
const IS_VISUALIZE_VIEW_DEFAULT = true;

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
    // padding: 0 16px;
`;

interface Props {
    properties?: { defaultDirection: LineageDirection };
    renderType: TabRenderType;
}

export function LineageTab({ properties, renderType }: Props) {
    const defaultDirection = properties?.defaultDirection || LineageDirection.Upstream;

    if (renderType === TabRenderType.COMPACT) {
        return <CompactLineageTab defaultDirection={defaultDirection} />;
    }
    return <WideLineageTab defaultDirection={defaultDirection} />;
}

function WideLineageTab({ defaultDirection }: { defaultDirection: LineageDirection }) {
    const [isVisualizeView, setIsVisualizeView] = useState(IS_VISUALIZE_VIEW_DEFAULT);
    const { isTabFullsize } = useContext(TabFullsizedContext);
    const { urn, entityType } = useEntityData();
    const isLineageV2 = useLineageV2();

    return (
        <LineageTabWrapper>
            {!isTabFullsize && (
                <LineageTabHeader>
                    <LineageSwitchWrapper>
                        <LineageViewSwitch selected={isVisualizeView} onClick={() => setIsVisualizeView(true)}>
                            Explorer
                        </LineageViewSwitch>
                        <LineageViewSwitch selected={!isVisualizeView} onClick={() => setIsVisualizeView(false)}>
                            Impact Analysis
                        </LineageViewSwitch>
                    </LineageSwitchWrapper>
                </LineageTabHeader>
            )}
            {!isVisualizeView && <LineageColumnView defaultDirection={defaultDirection} />}
            {isVisualizeView && !isLineageV2 && <LineageExplorer urn={urn} type={entityType} />}
            {isVisualizeView && isLineageV2 && <LineageEmbedded urn={urn} type={entityType} />}
        </LineageTabWrapper>
    );
}

function LineageEmbedded({ urn, type }: { urn: string; type: EntityType }) {
    return (
        <VisualizationWrapper>
            <LineageExplorerV2 urn={urn} type={type} embedded />
        </VisualizationWrapper>
    );
}

// TODO: Implement where does not trigger while searching
// eslint-disable-next-line @typescript-eslint/no-unused-vars
function useSetFullscreen() {
    const { setTabFullsize } = useContext(TabFullsizedContext);

    function handleKeyPress(e: KeyboardEvent) {
        if (e.key === 'f') {
            setTabFullsize((v) => !v);
        }
    }

    useEffect(() => {
        document.addEventListener('keydown', handleKeyPress);
        return () => {
            document.removeEventListener('keydown', handleKeyPress);
        };
    });
}
