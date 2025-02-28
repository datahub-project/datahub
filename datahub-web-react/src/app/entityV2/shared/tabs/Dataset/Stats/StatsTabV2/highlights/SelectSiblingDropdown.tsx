import { SimpleSelect } from '@src/alchemy-components';
import { Dataset } from '@src/types.generated';
import React, { useMemo } from 'react';
import { useGetSiblingsOptions } from './useGetSiblingsOptions';

interface Props {
    baseEntity: Dataset;
    selectedSiblingUrn: string | undefined;
    setSelectedSiblingUrn: React.Dispatch<React.SetStateAction<string | undefined>>;
}

const SelectSiblingDropdown = ({ baseEntity, selectedSiblingUrn, setSelectedSiblingUrn }: Props) => {
    const options = useGetSiblingsOptions({ baseEntityData: baseEntity });
    const siblingOptions = useMemo(() => options, [options]);

    const handleOptionClick = (value: string) => {
        setSelectedSiblingUrn(value);
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
