import { Divider } from 'antd';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import { Text } from '@components/components/Text';
import { colors } from '@components/theme';

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

export const BreadcrumbLink = styled(Link)`
    color: ${colors.gray[1800]};
    font-size: 12px;
    text-decoration: none;
    cursor: pointer;
`;

export const BreadcrumbButton = styled(Text)`
    cursor: pointer;

    :hover {
        color: ${colors.primary[500]};
    }
`;

export const VerticalDivider = styled(Divider)`
    color: ${colors.gray[100]};
    height: 16px;
    width: 2px;
    margin: 0 4px;
`;
