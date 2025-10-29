import { Divider } from 'antd';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { colors } from '@src/alchemy-components';

export const StyledDivider = styled(Divider)`
    border-color: ${colors.gray[100]};
    border-style: solid;
`;
