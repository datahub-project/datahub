import React from 'react';
import { PartitionOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { Tooltip } from '@components';
import { ShareResult } from '../../../../../../../types.generated';

const LineageIcon = styled(PartitionOutlined)<{ size?: number }>`
    font-size: ${(props) => props.size || 18}px;

    svg {
        font-size: ${(props) => props.size || 18}px;
        height: ${(props) => props.size || 18}px !important;
        width: ${(props) => props.size || 18}px !important;
    }
`;
interface Props {
    result: ShareResult;
    size?: number;
}

export default function SharedLineageIcon({ result, size }: Props) {
    const isSharingUpstream = result.shareConfig?.enableUpstreamLineage;
    const isSharingDownstream = result.shareConfig?.enableDownstreamLineage;

    let tooltipText = '';
    if (isSharingUpstream && isSharingDownstream) {
        tooltipText = 'Sharing assets upstream and downstream of this asset';
    } else if (isSharingUpstream) {
        tooltipText = 'Sharing assets upstream of this asset';
    } else if (isSharingDownstream) {
        tooltipText = 'Sharing assets downstream of this asset';
    }

    if (!isSharingDownstream && !isSharingDownstream) return null;

    return (
        <Tooltip title={tooltipText} overlayStyle={{ maxWidth: 260 }}>
            <LineageIcon size={size} />
        </Tooltip>
    );
}
