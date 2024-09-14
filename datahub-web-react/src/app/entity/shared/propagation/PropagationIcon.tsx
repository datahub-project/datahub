import styled from 'styled-components';
import { ThunderboltFilled } from '@ant-design/icons';
import { REDESIGN_COLORS } from '../constants';

export const PropagateThunderbolt = styled(ThunderboltFilled)`
    && {
        color: #a7c7fa;
    }
    font-size: 16px;
    &:hover {
        color: ${REDESIGN_COLORS.BLUE};
    }
    margin-right: 4px;
`;

export const PropagateThunderboltFilled = styled(ThunderboltFilled)`
    && {
        color: ${REDESIGN_COLORS.BLUE};
    }
    font-size: 16px;
    margin-right: 4px;
`;
