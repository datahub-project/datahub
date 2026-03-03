import { Divider } from 'antd';
import { Link } from 'react-router-dom';
import styled, { css } from 'styled-components';

import { Icon } from '@components/components/Icon';
import { Text } from '@components/components/Text';
import { colors } from '@components/theme';

const sharedTruncationStyles = css`
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
`;

export const Wrapper = styled.nav`
    display: flex;
    align-items: center;
    gap: 4px;
    overflow: hidden;
`;

export const BreadcrumbItemContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 4px;
    flex-wrap: nowrap;
    max-width: 100%;
    ${sharedTruncationStyles}
`;

export const BreadcrumbItemWrapper = styled.div<{ $isActive?: boolean }>`
    color: ${(props) => (props.$isActive ? props.theme.colors.text : props.theme.colors.textTertiary)} !important;
    ${sharedTruncationStyles}
`;

export const BreadcrumbLink = styled(Link)`
    color: inherit;
    font-size: 12px;
    text-decoration: none;
    cursor: pointer;
`;

export const BreadcrumbButton = styled(Text)`
    cursor: pointer;

    ${sharedTruncationStyles}

    :hover {
        color: ${colors.primary[500]};
    }
`;

export const VerticalDivider = styled(Divider)`
    color: ${(props) => props.theme.colors.border};
    height: 16px;
    width: 2px;
    margin: 0 4px;
`;

export const NoShrinkIcon = styled(Icon)`
    flex-shrink: 0;
`;
