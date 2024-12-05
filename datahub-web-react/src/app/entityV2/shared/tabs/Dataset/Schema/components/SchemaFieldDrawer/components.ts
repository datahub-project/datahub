import { Divider } from 'antd';
import styled from 'styled-components';
import { REDESIGN_COLORS } from '../../../../../constants';

export const SectionHeader = styled.div`
    margin-bottom: 8px;
    color: ${REDESIGN_COLORS.DARK_GREY};
    font-size: 14px;
    font-style: normal;
    font-weight: 700;
    line-height: 24px;
`;

export const StyledDivider = styled(Divider)`
    border-color: rgba(0, 0, 0, 0.3);
`;
