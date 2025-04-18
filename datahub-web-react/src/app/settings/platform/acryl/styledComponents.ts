import styled from 'styled-components';

import { ANTD_GRAY } from '@app/entity/shared/constants';

export const HeaderContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    height: 85px;
    padding: 20px;
`;

export const LeftContainer = styled.div`
    display: flex;
    align-items: center;
`;

export const HeaderTitle = styled.div`
    display: flex;
    font-size: 20px;
    font-weight: 700;
`;

export const HeaderSubtext = styled.div`
    font-size: 14px;
    font-weight: 400;
    color: ${ANTD_GRAY[7]};
`;
