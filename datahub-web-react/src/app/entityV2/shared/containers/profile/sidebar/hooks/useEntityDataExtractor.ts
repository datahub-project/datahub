import { get } from 'lodash';

import { useEntityData } from '@src/app/entity/shared/EntityContext';

interface UseEntityDataExtractorOptions {
    customPath?: string;
    defaultPath: string;
    arrayProperty: string; // 'tags' or 'terms'
}

export const useEntityDataExtractor = ({ customPath, defaultPath, arrayProperty }: UseEntityDataExtractorOptions) => {
    const { entityData } = useEntityData();

    const extractData = () => {
        if (customPath) {
            return get(entityData, customPath);
        }
        return get(entityData, defaultPath);
    };

    const data = extractData();
    const isEmpty = !data?.[arrayProperty]?.length;

    return { data, isEmpty };
};
