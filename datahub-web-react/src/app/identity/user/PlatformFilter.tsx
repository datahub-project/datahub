import React, { useMemo } from 'react';
import styled from 'styled-components/macro';

import { Platform } from '@app/identity/user/useAvailablePlatforms';
import { Pill, SimpleSelect, Text } from '@src/alchemy-components';

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

const CompactSelectWrapper = styled.div`
    & > div {
        max-height: 40px;
        height: 40px;
    }
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

    return (
        <CompactSelectWrapper className={className}>
            <SimpleSelect
                placeholder="Platform"
                position="end"
                options={options}
                values={selectedPlatforms}
                showClear
                showSearch
                isMultiSelect
                onUpdate={onPlatformChange}
                size="lg"
                width="fit-content"
                selectLabelProps={{
                    variant: 'custom',
                }}
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
                renderCustomSelectedValue={(option) => {
                    const platformOption = option as PlatformSelectOption;
                    // Only render the count display for the first selected option
                    const isFirstSelected = selectedPlatforms[0] === platformOption.value;
                    if (!isFirstSelected) return null;

                    return (
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                            <Text size="md">Platforms</Text>
                            <Pill label={selectedPlatforms.length.toString()} color="gray" size="sm" />
                        </div>
                    );
                }}
            />
        </CompactSelectWrapper>
    );
};
