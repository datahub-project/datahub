import { Tooltip } from '@components';
import styled from 'styled-components';

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
    color: ${(props) => props.$color ?? props.theme.colors.textTertiary};
    margin-right: 2px;
`;

export const StyledTooltip = styled(Tooltip)<{ maxWidth?: number }>`
    display: flex;
    white-space: nowrap;
    overflow: hidden;
    ${(props) => (props.maxWidth ? `max-width: ${props.maxWidth}px;` : '')}
`;
