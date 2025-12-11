/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import * as fixtures from '@graphql-mock/fixtures';
import { toLowerCaseEntityType, toTitleCase } from '@graphql-mock/helper';
import { EntityBrowseFn, GetBrowseResults } from '@graphql-mock/types';
import { BrowseInput } from '@types';

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
