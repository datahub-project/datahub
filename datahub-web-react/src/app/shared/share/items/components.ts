import MenuItem from 'antd/lib/menu/MenuItem';
import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';

export const StyledMenuItem = styled(MenuItem)`
    && {
        color: ${ANTD_GRAY[8]};
        background-color: ${ANTD_GRAY[1]};
    }
`;

export const TextSpan = styled.span`
    padding-left: 12px;
`;
