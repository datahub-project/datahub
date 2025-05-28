import React, { useMemo } from 'react';

import { useGetSiblingsOptions } from '@app/entityV2/shared/tabs/Dataset/Stats/StatsTabV2/highlights/useGetSiblingsOptions';
import { SimpleSelect } from '@src/alchemy-components';
import analytics, { EventType } from '@src/app/analytics';
import { extractPlatformNameFromAssetUrn } from '@src/app/entityV2/shared/utils';
import { Dataset } from '@src/types.generated';

interface Props {
    baseEntity: Dataset;
    selectedSiblingUrn: string | undefined;
    setSelectedSiblingUrn: React.Dispatch<React.SetStateAction<string | undefined>>;
}

const SelectSiblingDropdown = ({ baseEntity, selectedSiblingUrn, setSelectedSiblingUrn }: Props) => {
    const options = useGetSiblingsOptions({ baseEntityData: baseEntity });
    const siblingOptions = useMemo(() => options, [options]);

    const handleOptionClick = (siblingUrn: string) => {
        const platform = extractPlatformNameFromAssetUrn(siblingUrn);
        analytics.event({ type: EventType.FilterStatsPage, platform });
        setSelectedSiblingUrn(siblingUrn);
    };

    return (
        <div>
            <SimpleSelect
                options={siblingOptions}
                width="full"
                values={selectedSiblingUrn ? [selectedSiblingUrn] : undefined}
                showClear={false}
                onUpdate={(selectedValues) => handleOptionClick(selectedValues[0])}
            />
        </div>
    );
};

export default SelectSiblingDropdown;
