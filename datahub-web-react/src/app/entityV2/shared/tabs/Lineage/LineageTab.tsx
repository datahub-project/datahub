import { Icon } from '@components';
import { Rows } from '@phosphor-icons/react/dist/csr/Rows';
import { TreeStructure } from '@phosphor-icons/react/dist/csr/TreeStructure';
import React, { useCallback, useContext, useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { CompactLineageTab } from '@app/entityV2/shared/tabs/Lineage/CompactLineageTab';
import { LineageColumnView } from '@app/entityV2/shared/tabs/Lineage/LineageColumnView';
import { useLineageViewState } from '@app/entityV2/shared/tabs/Lineage/hooks';
import { TabRenderType } from '@app/entityV2/shared/types';
import { TabButtons } from '@app/homeV3/modules/shared/ButtonTabs/TabButtons';
import { Tab } from '@app/homeV3/modules/shared/ButtonTabs/types';
import LineageGraph from '@app/lineageV3/LineageGraph';
import TabFullsizedContext from '@app/shared/TabFullsizedContext';

import { LineageDirection } from '@types';

const LINEAGE_VIEW_EXPLORER = 'explorer';
const LINEAGE_VIEW_IMPACT = 'impact';

const LineageTabWrapper = styled.div`
    display: flex;
    flex-direction: column;
    height: 100%;
`;

const TabBarWrapper = styled.div`
    padding: 0 8px 8px;
`;

const TabLabel = styled.span`
    display: inline-flex;
    align-items: center;
    gap: 6px;
`;

const VisualizationWrapper = styled.div`
    display: flex;
    height: 100%;
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
    const { t } = useTranslation('lineage');
    const { isTabFullsize } = useContext(TabFullsizedContext);
    const { isVisualizeView, setVisualizeView, setVisualizeViewInEditMode } = useLineageViewState();

    const activeKey = isVisualizeView ? LINEAGE_VIEW_EXPLORER : LINEAGE_VIEW_IMPACT;

    const lineageViewTabs: Tab[] = useMemo(
        () => [
            {
                key: LINEAGE_VIEW_EXPLORER,
                label: (
                    <TabLabel>
                        <Icon icon={TreeStructure} size="lg" color="inherit" />
                        {t('viewSwitch.explorer')}
                    </TabLabel>
                ),
                dataTestId: 'lineage-view-explorer',
                content: (
                    <VisualizationWrapper>
                        <LineageGraph />
                    </VisualizationWrapper>
                ),
            },
            {
                key: LINEAGE_VIEW_IMPACT,
                label: (
                    <TabLabel>
                        <Icon icon={Rows} size="lg" color="inherit" />
                        {t('viewSwitch.impactAnalysis')}
                    </TabLabel>
                ),
                dataTestId: 'lineage-view-impact-analysis',
                content: (
                    <LineageColumnView
                        defaultDirection={defaultDirection}
                        setVisualizeViewInEditMode={setVisualizeViewInEditMode}
                    />
                ),
            },
        ],
        [t, defaultDirection, setVisualizeViewInEditMode],
    );

    const onLineageViewTabClick = useCallback(
        (key: string) => {
            setVisualizeView(key === LINEAGE_VIEW_EXPLORER);
        },
        [setVisualizeView],
    );

    const activeContent = lineageViewTabs.find((tab) => tab.key === activeKey)?.content;

    return (
        <LineageTabWrapper>
            {!isTabFullsize && (
                <TabBarWrapper>
                    <TabButtons tabs={lineageViewTabs} activeTab={activeKey} onTabClick={onLineageViewTabClick} />
                </TabBarWrapper>
            )}
            {activeContent}
        </LineageTabWrapper>
    );
}
