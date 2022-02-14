import { EntityRelationshipsResult, EntityRelationship, Entity, CorpGroupProperties} from '../../../types.generated';

export interface ExtendedEntityRelationshipsResult extends EntityRelationshipsResult {
    relationships: Array<ExtendedEntityRelationship>
}

interface  ExtendedEntityRelationship extends EntityRelationship {
    entity: ExtendedEntity
}

interface ExtendedEntity extends Entity {
    info: CorpGroupProperties;
}
