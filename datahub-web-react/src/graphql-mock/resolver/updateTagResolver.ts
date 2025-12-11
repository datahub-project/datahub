/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { createTag } from '@graphql-mock/fixtures/tag';
import { Tag } from '@types';

type UpdateTag = {
    data: { updateTag: Tag | undefined };
};

export const updateTagResolver = {
    updateTag({ variables: { input } }): UpdateTag {
        const tag: Tag = createTag(input);

        return {
            data: {
                updateTag: tag,
            },
        };
    },
};
