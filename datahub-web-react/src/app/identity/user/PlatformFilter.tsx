import React, { useMemo } from 'react';
import styled from 'styled-components/macro';

import { Platform } from '@app/identity/user/useAvailablePlatforms';
import { SimpleSelect, Text } from '@src/alchemy-components';

const PlatformIcon = styled.img`
    width: 16px;
    height: 16px;
    object-fit: contain;
    margin-right: 8px;
`;

const PlatformOption = styled.div`
    display: flex;
    align-items: center;
`;

type PlatformSelectOption = {
    value: string;
    label: string;
    platform: Platform;
};

type PlatformFilterProps = {
    selectedPlatforms: string[];
    onPlatformChange: (platforms: string[]) => void;
    platforms: Platform[];
    getPlatformIconUrl: (platformUrn: string) => string | null;
    className?: string;
};

export const PlatformFilter: React.FC<PlatformFilterProps> = ({
    selectedPlatforms,
    onPlatformChange,
    platforms,
    getPlatformIconUrl,
    className,
}) => {
    const options = useMemo(() => {
        return platforms.map((platform) => ({
            value: platform.id,
            label: platform.name,
            platform,
        }));
    }, [platforms]);

    const getDisplayText = () => {
        if (selectedPlatforms.length === 0) return 'Platform';
        if (selectedPlatforms.length === 1) return 'Platform';
        return 'Platforms';
    };

    return (
        <div className={className}>
            <SimpleSelect
                placeholder={getDisplayText()}
                position="end"
                options={options}
                values={selectedPlatforms}
                showClear
                showSearch
                isMultiSelect
                onUpdate={onPlatformChange}
                width="fit-content"
                size="lg"
                renderCustomOptionText={(option) => {
                    const platformOption = option as PlatformSelectOption;
                    const iconUrl = getPlatformIconUrl(platformOption.platform.urn);
                    return (
                        <PlatformOption>
                            {iconUrl && (
                                <PlatformIcon
                                    src={iconUrl}
                                    alt={platformOption.platform.name}
                                    title={platformOption.platform.name}
                                />
                            )}
                            <Text size="md">{platformOption.platform.name}</Text>
                        </PlatformOption>
                    );
                }}
            />
        </div>
    );
};
