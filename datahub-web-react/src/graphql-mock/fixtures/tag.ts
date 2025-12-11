/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import * as faker from 'faker';

import { findUserByURN } from '@graphql-mock/fixtures/searchResult/userSearchResult';
import { getActor } from '@graphql-mock/helper';
import { EntityType, Ownership, OwnershipType, Tag, TagUpdateInput } from '@types';

export const tagDb: Tag[] = [];

export const generateTag = (ownership?: Ownership): Tag => {
    const name = `${faker.company.bsNoun()}`;
    const description = `${faker.commerce.productDescription()}`;
    const tag: Tag = {
        urn: `urn:li:tag:${name}`,
        name,
        description,
        type: EntityType.Tag,
        ownership,
        __typename: 'Tag',
    };

    tagDb.push(tag);

    return tag;
};

export const createTag = ({ name, urn, description }: TagUpdateInput): Tag => {
    const user = findUserByURN(getActor());
    const tag: Tag = {
        urn,
        name,
        description,
        type: EntityType.Tag,
        ownership: {
            owners: [
                {
                    owner: user,
                    type: OwnershipType.Dataowner,
                    associatedUrn: urn,
                    __typename: 'Owner',
                },
            ],
            lastModified: { time: Date.now(), __typename: 'AuditStamp' },
            __typename: 'Ownership',
        },
        __typename: 'Tag',
    };

    tagDb.push(tag);

    return tag;
};
