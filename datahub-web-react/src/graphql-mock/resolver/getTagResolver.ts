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
