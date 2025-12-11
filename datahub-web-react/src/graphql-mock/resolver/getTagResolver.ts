/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { tagDb } from '@graphql-mock/fixtures/tag';
import { Tag } from '@types';

type GetTag = {
    data: { tag: Tag | undefined };
};

export const getTagResolver = {
    getTag({ variables: { urn } }): GetTag {
        const tag = tagDb.find((t) => {
            return t.urn === urn;
        });

        if (tag && !tag?.ownership) {
            tag.ownership = null;
        }

        return {
            data: {
                tag,
            },
        };
    },
};
