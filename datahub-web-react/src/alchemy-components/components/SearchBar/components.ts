import { colors, typography } from '@src/alchemy-components/theme';
import { Input } from 'antd';
import styled from 'styled-components';

export const StyledSearchBar = styled(Input)<{ $width?: string }>`
    height: 40px;
    width: ${(props) => props.$width};
    display: flex;
    align-items: center;
    border-radius: 8px;

    input {
        color: ${colors.gray[600]};
        font-size: ${typography.fontSizes.md} !important;
    }

    .ant-input-prefix {
        width: 20px;
        color: ${colors.gray[1800]};

        svg {
            height: 16px;
            width: 16px;
        }
    }

    &:hover,
    &:focus,
    &:focus-within {
        border-color: ${colors.violet[300]} !important;
        box-shadow: none !important;
    }
`;
