import { colors, typography } from '@src/alchemy-components/theme';
import { Input } from 'antd';
import styled from 'styled-components';

export const StyledSearchBar = styled(Input)<{ $width?: string }>`
    height: 40px;
    width: ${(props) => props.$width};
    display: flex;
    align-items: center;
    border-radius: 8px;
    border: 1px solid ${colors.gray[100]};
    box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07);
    transition: all 0.1s ease;

    &.ant-input-affix-wrapper {
        border: 1px solid ${colors.gray[100]};

        &:not(.ant-input-affix-wrapper-disabled) {
            &:hover {
                border-color: ${colors.gray[100]};
            }
        }

        &:focus,
        &-focused {
            border-color: ${colors.violet[500]};
            box-shadow: 0px 0px 0px 2px ${colors.violet[100]};
        }
    }

    input {
        color: ${colors.gray[600]};
        font-size: ${typography.fontSizes.md} !important;
        background-color: transparent;

        &::placeholder {
            color: ${colors.gray[400]};
        }
    }

    .ant-input-prefix {
        width: 20px;
        color: ${colors.gray[400]};
        margin-right: 4px;

        svg {
            height: 16px;
            width: 16px;
        }
    }

    &:hover {
        box-shadow: 0px 0px 0px 2px ${colors.violet[100]};
    }

    &.ant-input-affix-wrapper-focused {
        border-color: ${colors.violet[500]};
        box-shadow: 0px 0px 0px 2px ${colors.violet[100]};
    }
`;
