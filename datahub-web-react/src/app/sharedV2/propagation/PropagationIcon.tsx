import { ThunderboltFilled } from '@ant-design/icons';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';

export const PropagateThunderbolt = styled(ThunderboltFilled)<{ fontSize?: number }>`
    && {
        color: #a7c7fa;
    }
    font-size: ${(props) => props.fontSize || 16}px;
    &:hover {
        color: ${REDESIGN_COLORS.BLUE};
    }
    margin-right: 4px;
`;

export const PropagateThunderboltFilled = styled(ThunderboltFilled)<{ fontSize?: number }>`
    && {
        color: ${REDESIGN_COLORS.BLUE};
    }
    font-size: ${(props) => props.fontSize || 16}px;
    margin-right: 4px;
`;
