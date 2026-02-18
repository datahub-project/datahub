import { Divider } from 'antd';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { Text } from '@components/components/Text';

export const Wrapper = styled.nav`
    display: flex;
    align-items: center;
    gap: 4px;
`;

export const BreadcrumbItemContainer = styled.span`
    display: flex;
    align-items: center;
    gap: 4px;
`;

export const BreadcrumbLink = styled(Link)<{ $isCurrent?: boolean }>`
    color: ${(props) => (props.$isCurrent ? props.theme.colors.text : props.theme.colors.textTertiary)};
    font-size: 12px;
    text-decoration: none;
    cursor: pointer;
`;

export const BreadcrumbButton = styled(Text)<{ $isCurrent?: boolean }>`
    cursor: pointer;
    color: ${(props) => (props.$isCurrent ? props.theme.colors.text : props.theme.colors.textTertiary)};

    :hover {
        color: ${(props) => props.theme.styles['primary-color']};
    }
`;

export const VerticalDivider = styled(Divider)`
    color: ${(props) => props.theme.colors.border};
    height: 16px;
    width: 2px;
    margin: 0 4px;
`;
