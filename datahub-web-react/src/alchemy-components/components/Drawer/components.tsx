import { Drawer } from 'antd';
import styled from 'styled-components';

export const StyledDrawer = styled(Drawer)`
    .ant-drawer-header {
        padding: 16px;
        box-shadow: ${({ theme }) => theme.colors.shadowSm};
    }

    .ant-drawer-body {
        padding: 16px;
    }
` as typeof Drawer;

export const TitleContainer = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: space-between;
`;

export const TitleLeftContainer = styled.div`
    display: flex;
`;
