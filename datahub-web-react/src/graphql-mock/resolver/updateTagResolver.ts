import { Tag } from '../../types.generated';
import { createTag } from '../fixtures/tag';

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
