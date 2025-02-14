import styled from 'styled-components';
import { ANTD_GRAY, ANTD_GRAY_V2, REDESIGN_COLORS } from '../../entity/shared/constants';

export const Card = styled.div`
    background-color: ${ANTD_GRAY[1]};
    border: 2px solid ${ANTD_GRAY_V2[5]};
    border-radius: 8px;
    cursor: pointer;
    display: flex;
    align-items: center;

    :hover {
        border: 2px solid ${REDESIGN_COLORS.BLUE};
        cursor: pointer;
    }
`;
