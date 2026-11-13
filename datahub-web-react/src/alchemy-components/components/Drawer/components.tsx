import { Drawer } from 'antd';
import styled from 'styled-components';

// AntD's Drawer portals to document.body, escaping the app-wide `.themeV2 *`
// Mulish override. Apply Mulish here so every descendant — chrome and body —
// uses it instead of falling through to AntD's base @font-family (Manrope).
const MULISH_STACK = `'Mulish', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, 'Noto Sans', sans-serif`;

const toCssSize = (value: number | string) => (typeof value === 'number' ? `${value}px` : value);

export const StyledDrawer = styled(Drawer)<{ $bodyPadding: number | string; $hasCustomHeader: boolean }>`
    .ant-drawer-content,
    .ant-drawer-header,
    .ant-drawer-title,
    .ant-drawer-body {
        font-family: ${MULISH_STACK};
    }

    .ant-drawer-header {
        padding: 16px;
        box-shadow: ${({ theme }) => theme.colors.shadowSm};
    }

    ${({ $hasCustomHeader }) =>
        $hasCustomHeader &&
        `
        .ant-drawer-header {
            padding: 0;
            border-bottom: none;
            box-shadow: none;
        }

        .ant-drawer-title {
            flex: 1;
            min-width: 0;
        }
    `}

    .ant-drawer-body {
        padding: ${({ $bodyPadding }) => toCssSize($bodyPadding)};
    }

    .ant-drawer-footer {
        padding: 12px 16px;
        border-top: 1px solid ${({ theme }) => theme.colors.border};
    }
`;

export const TitleContainer = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: space-between;
`;

export const TitleLeftContainer = styled.div`
    display: flex;
`;
