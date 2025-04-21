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
