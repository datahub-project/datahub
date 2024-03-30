import React, { useContext } from 'react';
import { Handle, NodeProps, Position } from 'reactflow';
import styled from 'styled-components';
import OverflowTitle from '../../sharedV2/text/OverflowTitle';
import { FetchStatus, getNodeColor, LineageDisplayContext, LineageEntity, LineageNodesContext } from '../common';
import { EntityType, LineageDirection } from '../../../types.generated';
import getTypeIcon from '../../sharedV2/icons/getTypeIcon';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ANTD_GRAY } from '../../entityV2/shared/constants';
import { ExpandLineageButton } from '../LineageEntityNode/ExpandLineageButton';

export const LINEAGE_WORKBOOK_NODE_NAME = 'tableau-workbook';
export const WORKBOOK_NODE_MAX_WIDTH = 100;

const NodeWrapper = styled.div<{ selected: boolean; color: string }>`
    align-items: center;
    background-color: white;
    border: 1px solid ${({ color, selected }) => (selected ? color : ANTD_GRAY[4.5])};
    border-radius: 6px;
    display: flex;
    flex-direction: column;
    overflow-y: hidden;
    max-width: ${WORKBOOK_NODE_MAX_WIDTH}px;
    cursor: pointer;
`;

const FakeCard = styled.div`
    height: 0;
    position: absolute;
    width: 100%;
    top: 50%;
`;

const CardWrapper = styled.div`
    display: flex;
    flex-direction: column;
    padding: 6px 9px;
    width: 100%;
`;

const EntityTypeShadow = styled.div<{ color: string }>`
    background: ${({ color }) => color};
    border-radius: 6px;
    position: absolute;

    height: 100%;
    width: 22px;

    left: -3px;
    top: 0;
    z-index: -1;
`;

const CustomHandle = styled(Handle)<{ position: Position }>`
    background: initial;
    border: initial;
    ${({ position }) => (position === Position.Left ? 'left: -3px; top: 50%;' : 'right: 0; top: 50%;')}
`;

const IconsWrapper = styled.div`
    align-items: center;
    color: ${ANTD_GRAY[10]};
    display: flex;
    flex-direction: row;
    font-size: 16px;
    gap: 4px;
`;

const PlatformIcon = styled.img`
    height: 1em;
    width: 1em;
`;

const MainTextWrapper = styled.div`
    display: flex;
    flex-direction: column;
    flex-grow: 1;
    height: 100%;
    min-width: 0;
    margin-top: 4px;
`;

const Title = styled(OverflowTitle)`
    font-weight: 600;
    font-size: 8px;
    line-height: 125%;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
`;

// Note: This component has been altered from when it originally worked and may need changes to work again.
export default function TableauWorkbookNode(props: NodeProps<LineageEntity>) {
    const { data, selected } = props;
    const { urn, type, fetchStatus } = data;

    const { nodes } = useContext(LineageNodesContext);
    const { setHoveredNode } = useContext(LineageDisplayContext);
    const entity = nodes.get(urn)?.entity;

    const entityRegistry = useEntityRegistry();
    const platformName = entityRegistry.getDisplayName(EntityType.DataPlatform, entity?.platform);
    const [nodeColor] = getNodeColor(type);

    return (
        <NodeWrapper selected={selected} color={nodeColor}>
            <EntityTypeShadow color={nodeColor} />
            <FakeCard>
                {[FetchStatus.UNFETCHED, FetchStatus.LOADING].includes(fetchStatus[LineageDirection.Upstream]) && (
                    <ExpandLineageButton
                        urn={urn}
                        direction={LineageDirection.Upstream}
                        display={fetchStatus[LineageDirection.Upstream] === FetchStatus.UNFETCHED}
                    />
                )}
                {[FetchStatus.UNFETCHED, FetchStatus.LOADING].includes(fetchStatus[LineageDirection.Downstream]) && (
                    <ExpandLineageButton
                        urn={urn}
                        direction={LineageDirection.Downstream}
                        display={fetchStatus[LineageDirection.Downstream] === FetchStatus.UNFETCHED}
                    />
                )}
            </FakeCard>
            <CardWrapper onMouseEnter={() => setHoveredNode(urn)} onMouseLeave={() => setHoveredNode(null)}>
                <CustomHandle type="target" position={Position.Left} isConnectable={false} />
                {entity?.icon && (
                    <>
                        <IconsWrapper>
                            <PlatformIcon src={entity.icon} alt={platformName || 'platform'} title={platformName} />
                            {getTypeIcon(entityRegistry, entity.type, entity.subtype, true)}
                        </IconsWrapper>
                    </>
                )}
                <MainTextWrapper>
                    <Title title={entity?.name} />
                </MainTextWrapper>
                <CustomHandle type="source" position={Position.Right} isConnectable={false} />
            </CardWrapper>
        </NodeWrapper>
    );
}
