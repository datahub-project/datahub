import { FolderOutlined, RightOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import useContentTruncation from '@app/shared/useContentTruncation';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, GlossaryNode } from '@types';

const StyledRightOutlined = styled(RightOutlined)`
    color: ${ANTD_GRAY[7]};
    font-size: 8px;
    margin: 0 4px;
`;

// must display content in reverse to have ellipses at the beginning of content
export const ParentNodesWrapper = styled.div`
    align-items: center;
    white-space: nowrap;
    text-overflow: ellipsis;
    flex-direction: row-reverse;
    display: flex;
    max-width: 460px;
    text-overflow: ellipsis;
    overflow: hidden;
`;

export const Ellipsis = styled.span<{ $color?: string }>`
    color: ${(props) => props.$color ?? ANTD_GRAY[7]};
    margin-right: 2px;
`;

export const StyledTooltip = styled(Tooltip)<{ maxWidth?: number }>`
    display: flex;
    white-space: nowrap;
    overflow: hidden;
    ${(props) => (props.maxWidth ? `max-width: ${props.maxWidth}px;` : '')}
`;

const GlossaryNodeText = styled(Typography.Text)<{ color: string }>`
    font-size: 12px;
    line-height: 20px;
    color: ${(props) => props.color};
`;

const GlossaryNodeIcon = styled(FolderOutlined)<{ color: string }>`
    color: ${(props) => props.color};

    &&& {
        font-size: 12px;
        margin-right: 4px;
    }
`;

interface Props {
    parentNodes?: GlossaryNode[] | null;
}
