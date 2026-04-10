import { Document, Domain, Entity, EntityType, GlossaryTerm } from '@types';

export function getParentEntities(entity: Entity): Entity[] | null {
    if (!entity) {
        return null;
    }
    if (entity.type === EntityType.GlossaryTerm || entity.type === EntityType.GlossaryNode) {
        return (entity as GlossaryTerm).parentNodes?.nodes || [];
    }
    if (entity.type === EntityType.Domain) {
        return (entity as Domain).parentDomains?.domains || [];
    }
    if (entity.type === EntityType.Document) {
        // Document type is generated and includes parentDocuments field
        return (entity as Document).parentDocuments?.documents || [];
    }
    return null;
}
