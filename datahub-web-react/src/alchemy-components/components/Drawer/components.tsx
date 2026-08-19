import { Drawer } from 'antd';
import styled from 'styled-components';

// AntD's Drawer portals to document.body, escaping the app-wide `.themeV2 *`
// Mulish override. Apply Mulish here so every descendant — chrome and body —
// uses it instead of falling through to AntD's base @font-family (Manrope).
const MULISH_STACK = `'Mulish', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, 'Noto Sans', sans-serif`;

export const StyledDrawer = styled(Drawer)`
    .ant-drawer-content,
    .ant-drawer-header,
    .ant-drawer-body {
        font-family: ${MULISH_STACK};
    }

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
