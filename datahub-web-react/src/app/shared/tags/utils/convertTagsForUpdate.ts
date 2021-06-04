import { TagAssociation, TagAssociationUpdate } from '../../../../types.generated';

export function convertTagsForUpdate(tags: TagAssociation[]): TagAssociationUpdate[] {
    return tags
        .map((tag) => ({
            tag: { urn: tag?.tag?.urn, name: tag?.tag?.name, description: tag?.tag?.description },
        }))
        .filter((tag) => !!tag?.tag.urn) as TagAssociationUpdate[];
}
