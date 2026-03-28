import React from 'react';
import styled, { CSSObject, useTheme } from 'styled-components';

import PlatformIcon from '@app/sharedV2/icons/PlatformIcon';

import { DataPlatform } from '@types';

const Container = styled.div`
    display: flex;
    align-items: center;
    position: relative;
`;

const secondIconStyles = (isSmall: boolean, bgColor: string) => ({
    marginLeft: isSmall ? '-10px' : '-16px',
    zIndex: 0,
    borderRadius: isSmall ? '8px' : '16px',
    border: `1px solid ${bgColor}`,
    padding: isSmall ? '4px' : '10px',
});

const firstIconStyles = (isSmall: boolean, bgColor: string) => ({
    zIndex: 1,
    borderRadius: isSmall ? '8px' : '16px',
    border: `1px solid ${bgColor}`,
    padding: isSmall ? '4px' : '10px',
});

const SMALL_ICON_SIZE = 20;

interface Props {
    platforms: DataPlatform[];
    size?: number;
    styles?: CSSObject | undefined;
}

const StackImages = ({ platforms, size = 28, styles }: Props) => {
    const theme = useTheme();
    const bgColor = theme.colors.bg;
    const uniquePlatforms = platforms.reduce<DataPlatform[]>((acc, current) => {
        if (!acc.find((platform) => platform.urn === current.urn)) {
            acc.push(current);
        }
        return acc;
    }, []);

    const areIconsSmall = size < SMALL_ICON_SIZE;

    return (
        <Container>
            {uniquePlatforms.slice(0, 2).map((platform, index) => (
                <>
                    {index === 1 ? (
                        <PlatformIcon
                            platform={platform}
                            size={size}
                            styles={
                                styles
                                    ? { ...secondIconStyles(areIconsSmall, bgColor), ...styles }
                                    : secondIconStyles(areIconsSmall, bgColor)
                            }
                        />
                    ) : (
                        <PlatformIcon
                            platform={platform}
                            size={size}
                            styles={
                                styles
                                    ? { ...firstIconStyles(areIconsSmall, bgColor), ...styles }
                                    : firstIconStyles(areIconsSmall, bgColor)
                            }
                        />
                    )}
                </>
            ))}
        </Container>
    );
};

export default StackImages;
