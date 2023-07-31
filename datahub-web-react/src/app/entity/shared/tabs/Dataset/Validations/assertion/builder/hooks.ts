import { useEffect } from 'react';
import { DatasetFreshnessSourceType } from '../../../../../../../../types.generated';
import { getFreshnessSourceOption } from './utils';

type ChangeSourceOptionContext = {
    sourceType: DatasetFreshnessSourceType;
    updateSourceType: (value: string) => void;
};

// Custom hook to update the source type when a condition is met
export const useChangeSourceOptionIf = (
    condition: boolean,
    { sourceType, updateSourceType }: ChangeSourceOptionContext,
) => {
    useEffect(() => {
        if (condition) {
            const sourceOption = getFreshnessSourceOption(sourceType);
            updateSourceType(sourceOption.name);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [condition]);
};
