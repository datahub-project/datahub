import * as fixtures from '../fixtures';
import { BrowseInput } from '../../types.generated';
import { EntityBrowseFn, GetBrowseResults } from '../types';
import { toLowerCaseEntityType, toTitleCase } from '../helper';

const toPathTitle = (paths: string[]): string => {
    return paths?.map((p) => toTitleCase(p)).join('');
};

export const getBrowseResultsResolver = {
    getBrowseResults({ variables: { input } }): GetBrowseResults {
        const { type, path = [], start = 0, count = 0 }: BrowseInput = input;
        const startValue = start as number;
        const countValue = count as number;
        const paths = path as string[];
        const entityType = toLowerCaseEntityType(type);
        const pathTitle = toPathTitle(paths);

        const result: GetBrowseResults | EntityBrowseFn =
            fixtures[`${entityType}BrowseResult`][`${entityType}Browse${pathTitle}`];

        if (typeof result === 'function') {
            return result({ start: startValue, count: countValue, path: paths });
        }
        return result;
    },
};
