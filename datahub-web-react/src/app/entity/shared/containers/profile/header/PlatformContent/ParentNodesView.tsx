import { FolderOutlined, RightOutlined } from '@ant-design/icons';
import { Tooltip, Typography } from 'antd';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';

import { GlossaryNode } from '@types';

export const StyledRightOutlined = styled(RightOutlined)`
    color: ${ANTD_GRAY[7]};
    font-size: 8px;
    margin: 0 10px;
`;

// must display content in reverse to have ellipses at the beginning of content
export const ParentNodesWrapper = styled.div`
    align-items: center;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    flex-direction: row-reverse;
    display: flex;
`;

export const Ellipsis = styled.span`
    color: ${ANTD_GRAY[7]};
    margin-right: 2px;
`;

export const StyledTooltip = styled(Tooltip)`
    display: flex;
    white-space: nowrap;
    overflow: hidden;
`;
