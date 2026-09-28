import { RightOutlined } from '@ant-design/icons';
import { Tooltip } from '@components';
import styled from 'styled-components';

export const StyledRightOutlined = styled(RightOutlined)`
    color: ${(props) => props.theme.colors.textTertiary};
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
    color: ${(props) => props.theme.colors.textTertiary};
    margin-right: 2px;
`;

export const StyledTooltip = styled(Tooltip)`
    display: flex;
    white-space: nowrap;
    overflow: hidden;
`;
