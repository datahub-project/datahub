import React, { useContext } from 'react';
import { NodeProps } from 'reactflow';
import styled from 'styled-components';
import { HomeOutlined } from '@ant-design/icons';
import { LineageEntity, LineageNodesContext } from '../common';
import NodeContents from './NodeContents';
import { LINEAGE_COLORS } from '../../entityV2/shared/constants';

export const LINEAGE_ENTITY_NODE_NAME = 'lineage-entity';

const HomeNodeBubble = styled.div`
    align-items: center;
    background-color: ${LINEAGE_COLORS.PURPLE_3};
    border-radius: 10px;
    color: white;
    display: flex;
    font-size: 10px;
    font-weight: 600;
    height: 22px;
    justify-content: center;
    left: 1px;
    padding: 4px 8px;
    position: absolute;
    top: -26px;
`;

export default function LineageEntityNode(props: NodeProps<LineageEntity>) {
    const { data, selected } = props;
    const { urn, type } = data;
    const { nodes, rootUrn } = useContext(LineageNodesContext);
    // TODO: Figure out why Apollo caching is not working for useEntityLineage
    const entity = nodes.get(urn)?.entity; // useEntityLineage(urn);

    return (
        <>
            {urn === rootUrn && (
                <HomeNodeBubble>
                    <HomeOutlined style={{ marginRight: 4 }} />
                    Home
                </HomeNodeBubble>
            )}
            <NodeContents {...data} urn={urn} type={type} selected={selected} entity={entity} />
        </>
    );
}
