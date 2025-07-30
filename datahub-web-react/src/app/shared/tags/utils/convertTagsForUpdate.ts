import { TagAssociation, TagAssociationUpdate } from '@types';

export function convertTagsForUpdate(tags: TagAssociation[]): TagAssociationUpdate[] {
    return tags.map((tag) => ({
        tag: { urn: tag.tag.urn, name: tag.tag.name, description: tag.tag.description },
    }));
}
