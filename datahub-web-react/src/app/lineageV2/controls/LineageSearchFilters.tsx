import { Switch, Tooltip } from '@components';
import React, { useContext, useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { LineageNodesContext, isTransformational } from '@app/lineageV2/common';
import { ControlPanel, ControlPanelSubtext, ControlPanelTitle } from '@app/lineageV2/controls/common';
import InfoPopover from '@app/sharedV2/icons/InfoPopover';

const ToggleWrapper = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    justify-content: space-between;
    width: 100%;
    gap: 10px;
`;

const ToggleLabel = styled.span`
    display: flex;
    align-items: center;
    gap: 4px;
    font-size: 12px;
    font-weight: 400;
    line-height: 16px;
    color: ${(props) => props.theme.colors.text};
`;

const StyledInfoPopover = styled(InfoPopover)`
    position: relative;
    color: ${(props) => props.theme.colors.textTertiary};
`;

const PopoverWrapper = styled.div`
    max-width: 200px;
`;

export default function LineageSearchFilters() {
    const { t } = useTranslation('lineage');
    const {
        nodes,
        rootUrn,
        nodeVersion,
        hideTransformations,
        setHideTransformations,
        showDataProcessInstances,
        setShowDataProcessInstances,
        showGhostEntities,
        setShowGhostEntities,
    } = useContext(LineageNodesContext);

    const hasTransformations = useMemo(
        () => Array.from(nodes.values()).some((node) => node.urn !== rootUrn && isTransformational(node)), // eslint-disable-next-line react-hooks/exhaustive-deps
        [nodes, nodeVersion],
    );
    return (
        <ControlPanel>
            <ControlPanelTitle>{t('controls.filters.title')}</ControlPanelTitle>
            <ControlPanelSubtext>{t('controls.filters.description')}</ControlPanelSubtext>
            <ToggleWrapper>
                <ToggleLabel>
                    {t('controls.filters.hideTransformations.label')}
                    <StyledInfoPopover
                        content={<PopoverWrapper>{t('controls.filters.hideTransformations.tooltip')}</PopoverWrapper>}
                    />
                </ToggleLabel>
                <Tooltip title={hasTransformations ? undefined : t('controls.filters.noTransformationsToHide.tooltip')}>
                    <Switch
                        label=""
                        labelStyle={{ display: 'none' }}
                        isDisabled={!hasTransformations}
                        isChecked={hideTransformations}
                        onChange={() => setHideTransformations(!hideTransformations)}
                    />
                </Tooltip>
            </ToggleWrapper>
            <ToggleWrapper>
                <ToggleLabel>
                    {t('controls.filters.hideProcessInstances.label')}
                    <StyledInfoPopover
                        content={<PopoverWrapper>{t('controls.filters.hideProcessInstances.tooltip')}</PopoverWrapper>}
                    />
                </ToggleLabel>
                <Switch
                    label=""
                    labelStyle={{ display: 'none' }}
                    isChecked={!showDataProcessInstances}
                    onChange={() => setShowDataProcessInstances(!showDataProcessInstances)}
                />
            </ToggleWrapper>
            <ToggleWrapper>
                <ToggleLabel>
                    {t('controls.filters.showHiddenEdges.label')}
                    <StyledInfoPopover
                        content={<PopoverWrapper>{t('controls.filters.showHiddenEdges.tooltip')}</PopoverWrapper>}
                    />
                </ToggleLabel>
                <Switch
                    label=""
                    labelStyle={{ display: 'none' }}
                    isChecked={showGhostEntities}
                    onChange={() => setShowGhostEntities(!showGhostEntities)}
                />
            </ToggleWrapper>
        </ControlPanel>
    );
}
