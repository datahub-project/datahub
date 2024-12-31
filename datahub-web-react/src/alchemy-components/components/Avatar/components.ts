import { colors } from '@src/alchemy-components/theme';
import { AvatarSizeOptions } from '@src/alchemy-components/theme/config';
import styled from 'styled-components';
import { getAvatarColorStyles, getAvatarNameSizes, getAvatarSizes } from './utils';

export const Container = styled.div<{ $hasOnClick: boolean; $showInPill?: boolean }>`
    display: inline-flex;
    align-items: center;
    gap: 4px;
    border-radius: 20px;
    border: ${(props) => props.$showInPill && `1px solid ${colors.gray[100]}`};
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
    $color: string;
    $size?: AvatarSizeOptions;
    $isOutlined?: boolean;
    $hasImage?: boolean;
}>`
    ${(props) => getAvatarSizes(props.$size)}

    border-radius: 50%;
    color: ${(props) => props.$color};
    border: ${(props) => props.$isOutlined && `1px solid ${colors.gray[1800]}`};
    display: flex;
    align-items: center;
    justify-content: center;
    ${(props) => !props.$hasImage && getAvatarColorStyles(props.$color)}
`;

export const AvatarImage = styled.img`
    width: 100%;
    height: 100%;
    object-fit: cover;
    border-radius: 50%;
`;

export const AvatarText = styled.span<{ $size?: AvatarSizeOptions }>`
    color: ${colors.gray[1700]};
    font-weight: 600;
    font-size: ${(props) => getAvatarNameSizes(props.$size)};
`;
