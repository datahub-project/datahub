import styled from 'styled-components';

import { Input } from '@src/alchemy-components';

export const StyledInput = styled(Input)`
    width: auto;
    background: ${(props) => props.theme.colors.bg};
    font-size: 14px;
    font-weight: 500;
    line-height: 24px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

export const MatchLabelText = styled.span`
    font-size: 12px;
    font-weight: 400;
    color: ${(props) => props.theme.colors.textSecondary};
    padding: 4px 4px;
`;

export const SearchContainer = styled.div`
    position: relative;
    --antd-wave-shadow-color: transparent;
    flex: auto;
    display: flex;
    flex-grow: 0;
    align-items: start;
    white-space: nowrap;
    flex-direction: column;

    .ant-input-group-wrapper {
        border-radius: 8px;
        border: 1px solid ${(props) => props.theme.colors.border};
        background: ${(props) => props.theme.colors.bgSurface};
    }

    .ant-input-group-wrapper {
        background-color: ${(props) => props.theme.colors.bgSurface} !important;
    }

    .ant-input-wrapper {
        background-color: transparent !important;
    }

    .ant-input {
        border-radius: 0;
        color: ${(props) => props.theme.colors.textSecondary};
    }
    .ant-input::placeholder {
        color: ${(props) => props.theme.colors.textSecondary} !important;
        opacity: 1;
    }

    .ant-input-affix-wrapper {
        border-radius: 8px;
        border: 1px solid ${(props) => props.theme.colors.border};
        transition: border-color 0.3s ease-in-out;
        cursor: text !important;
    }

    .ant-input-group-addon {
        border: none;
        background-color: transparent !important;
        left: 2px;
    }

    .ant-input-affix-wrapper:focus-within,
    .ant-input-affix-wrapper:not(.ant-input-affix-wrapper-disabled):hover {
        border: 1px solid ${(props) => props.theme.styles['primary-color']};
    }

    .ant-input-affix-wrapper::selection {
        background: transparent;
    }
`;
