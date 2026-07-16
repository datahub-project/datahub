import { Input } from 'antd';
import styled from 'styled-components';

import { typography } from '@src/alchemy-components/theme';

export const StyledSearchBar = styled(Input)<{ $width?: string; $height?: string }>`
    height: ${(props) => props.$height};
    width: ${(props) => props.$width};
    display: flex;
    align-items: center;
    border-radius: 8px;
    border: 1px solid ${(props) => props.theme.colors.border};
    background-color: ${(props) => props.theme.colors.bg};
    box-shadow: ${(props) => props.theme.colors.shadowXs};
    transition: all 0.1s ease;

    &.ant-input-affix-wrapper {
        border: 1px solid ${(props) => props.theme.colors.border};
        background-color: ${(props) => props.theme.colors.bg};

        &:not(.ant-input-affix-wrapper-disabled) {
            &:hover {
                border-color: ${(props) => props.theme.colors.border};
            }
        }

        &:focus,
        &-focused {
            border-color: ${(props) => props.theme.colors.borderBrand};
            box-shadow: 0px 0px 0px 2px ${(props) => props.theme.colors.borderBrandFocused};
        }
    }

    input {
        color: ${(props) => props.theme.colors.text};
        font-size: ${typography.fontSizes.md} !important;
        background-color: transparent;

        &::placeholder {
            color: ${(props) => props.theme.colors.textPlaceholder};
        }
    }

    .ant-input-prefix {
        width: 20px;
        color: ${(props) => props.theme.colors.icon};
        margin-right: 4px;

        svg {
            height: 16px;
            width: 16px;
        }
    }

    &:hover,
    &:focus,
    &:focus-within {
        border-color: ${({ theme }) => theme.colors.borderBrandFocused} !important;
        box-shadow: none !important;
    }

    &.ant-input-affix-wrapper-focused {
        border-color: ${(props) => props.theme.colors.borderBrand};
        box-shadow: 0px 0px 0px 2px ${(props) => props.theme.colors.borderBrandFocused};
    }
`;
