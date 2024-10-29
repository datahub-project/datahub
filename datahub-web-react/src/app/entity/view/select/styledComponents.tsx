import { RightOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../shared/constants';

export const NoMarginButton = styled(Button)`
    && {
        margin: 0px;
    }
`;

export const StyledRightOutlined = styled(RightOutlined)`
    && {
        font-size: 8px;
        color: ${ANTD_GRAY[7]};
    }
`;
