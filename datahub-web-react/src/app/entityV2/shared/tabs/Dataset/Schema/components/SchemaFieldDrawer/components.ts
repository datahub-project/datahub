import { Divider } from 'antd';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { colors } from '@src/alchemy-components';

export const SectionHeader = styled.div`
    margin-bottom: 8px;
    color: ${REDESIGN_COLORS.DARK_GREY};
    font-size: 14px;
    font-style: normal;
    font-weight: 700;
    line-height: 24px;
`;

export const StyledDivider = styled(Divider)`
    border-color: ${colors.gray[100]};
    border-style: solid;
`;
