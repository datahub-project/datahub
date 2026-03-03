import { Tooltip } from '@components';
import { Switch } from 'antd';
import React, { useContext, useMemo } from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { LineageNodesContext, isTransformational, useIgnoreSchemaFieldStatus } from '@app/lineageV3/common';
import { ControlPanel, ControlPanelSubtext, ControlPanelTitle } from '@app/lineageV3/controls/common';
import InfoPopover from '@app/sharedV2/icons/InfoPopover';

import { EntityType } from '@types';

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
    color: ${ANTD_GRAY[9]};
`;

const StyledInfoPopover = styled(InfoPopover)`
    position: relative;
    color: ${ANTD_GRAY[7]};
`;

const PopoverWrapper = styled.div`
    max-width: 200px;
`;

const StyledSwitch = styled(Switch)``;

export default function LineageSearchFilters() {
    const {
        nodes,
        rootUrn,
        rootType,
        nodeVersion,
        hideTransformations,
        setHideTransformations,
        showDataProcessInstances,
        setShowDataProcessInstances,
        showGhostEntities,
        setShowGhostEntities,
    } = useContext(LineageNodesContext);
    const ignoreSchemaFieldStatus = useIgnoreSchemaFieldStatus();

    const mustShowGhostEntities = rootType === EntityType.SchemaField && ignoreSchemaFieldStatus;

    const hasTransformations = useMemo(
        () => Array.from(nodes.values()).some((node) => node.urn !== rootUrn && isTransformational(node, rootType)),
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [rootUrn, rootType, nodes, nodeVersion],
    );
    return (
        <ControlPanel>
            <ControlPanelTitle>Filters</ControlPanelTitle>
            <ControlPanelSubtext>Hide or show assets on the graph.</ControlPanelSubtext>
            <ToggleWrapper>
                <span>
                    <ToggleLabel>
                        Hide Transformations
                        <StyledInfoPopover
                            content={
                                <PopoverWrapper>
                                    Hide queries and transforms (circular nodes). Will not hide home node.
                                </PopoverWrapper>
                            }
                        />
                    </ToggleLabel>
                </span>
                <Tooltip title={hasTransformations ? undefined : 'No transformations to hide'}>
                    <StyledSwitch
                        disabled={!hasTransformations}
                        size="small"
                        checked={hideTransformations}
                        onChange={setHideTransformations}
                    />
                </Tooltip>
            </ToggleWrapper>
            <ToggleWrapper>
                <span>
                    <ToggleLabel>
                        Hide Process Instances
                        <StyledInfoPopover
                            content={<PopoverWrapper>Hide task runs. Will not hide home node.</PopoverWrapper>}
                        />
                    </ToggleLabel>
                </span>
                <StyledSwitch
                    size="small"
                    checked={!showDataProcessInstances}
                    onChange={() => setShowDataProcessInstances(!showDataProcessInstances)}
                />
            </ToggleWrapper>
            <ToggleWrapper>
                <span>
                    <ToggleLabel>
                        Show Hidden Edges
                        <StyledInfoPopover
                            content={
                                <PopoverWrapper>
                                    Show assets that have been deleted or do not exist in DataHub
                                </PopoverWrapper>
                            }
                        />
                    </ToggleLabel>
                </span>
                <Tooltip title={mustShowGhostEntities ? 'Required when viewing column lineage' : undefined}>
                    <StyledSwitch
                        disabled={mustShowGhostEntities}
                        size="small"
                        checked={showGhostEntities}
                        onChange={setShowGhostEntities}
                    />
                </Tooltip>
            </ToggleWrapper>
        </ControlPanel>
    );
}
