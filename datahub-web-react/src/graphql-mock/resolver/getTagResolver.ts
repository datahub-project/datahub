import { Tag } from '../../types.generated';
import { tagDb } from '../fixtures/tag';

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
