import { Drawer } from 'antd';
import styled from 'styled-components';

export const StyledDrawer = styled(Drawer)`
    .ant-drawer-header {
        padding: 16px;
        box-shadow: 0px 0px 6px 0px rgba(93, 102, 139, 0.2);
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
