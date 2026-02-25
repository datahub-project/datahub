import styled from 'styled-components';

import {
    AvatarColorVariant,
    getAvatarColorStyles,
    getAvatarNameSizes,
    getAvatarSizes,
} from '@components/components/Avatar/utils';

import { AvatarSizeOptions } from '@src/alchemy-components/theme/config';

export const Container = styled.div<{ $hasOnClick: boolean; $showInPill?: boolean }>`
    display: inline-flex;
    align-items: center;
    gap: 4px;
    border-radius: 20px;
    border: ${(props) => props.$showInPill && `1px solid ${props.theme.colors.border}`};
    padding: ${(props) => props.$showInPill && '3px 6px 3px 4px'};

    ${(props) =>
        props.$hasOnClick &&
        `
        :hover {
        cursor: pointer;
    }
        `}
`;

export const AvatarImageWrapper = styled.div<{
    $variant: AvatarColorVariant;
    $size?: AvatarSizeOptions;
    $isOutlined?: boolean;
}>`
    ${(props) => getAvatarSizes(props.$size)}

    position: relative;
    border-radius: 50%;
    color: ${(props) => props.theme.colors.textSecondary};
    border: ${(props) => props.$isOutlined && `1px solid ${props.theme.colors.border}`};
    display: flex;
    align-items: center;
    justify-content: center;
    ${(props) => getAvatarColorStyles(props.$variant, props.theme.colors)}
`;

export const AvatarImage = styled.img`
    width: 100%;
    height: 100%;
    object-fit: cover;
    border-radius: 50%;
`;

export const AvatarText = styled.span<{ $size?: AvatarSizeOptions }>`
    color: ${(props) => props.theme.colors.textSecondary};
    font-weight: 600;
    font-size: ${(props) => getAvatarNameSizes(props.$size)};
`;
