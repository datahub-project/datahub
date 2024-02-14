import styled from 'styled-components';
import { Button } from 'antd';

export const StyledPanelButton = styled(Button)`
    padding-top: 8px;
    padding-bottom: 8px;
    padding-left: 16px;
    padding-right: 16px;
    display: flex;
    align-items: center;
    width: 100%;

    .anticon {
        margin-bottom: -2px;
    }
`;
