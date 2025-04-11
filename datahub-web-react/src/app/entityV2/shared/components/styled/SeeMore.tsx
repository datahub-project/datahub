import { Button } from 'antd';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../constants';

export const SeeMore = styled(Button)`
    margin-top: -20px;
    background-color: ${ANTD_GRAY[4]};
    padding: 8px;
    border: none;
    line-height: 8px;
    height: 20px;
`;
