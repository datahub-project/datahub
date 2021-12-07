import { EntityAndType } from '../types';
import { EntityRelationshipsResult, RelationshipDirection } from '../../../types.generated';
import { FORWARD_RELATIONSHIPS, INVERSE_RELATIONSHIPS } from '../constants';

export function getChildrenFromRelationships({
    incomingRelationships,
    outgoingRelationships,
    direction,
}: {
    incomingRelationships: EntityRelationshipsResult | null | undefined;
    outgoingRelationships: EntityRelationshipsResult | null | undefined;
    direction: RelationshipDirection;
}) {
    return [
        ...(incomingRelationships?.relationships || []).filter((relationship) => {
            if (FORWARD_RELATIONSHIPS.indexOf(relationship.type) >= 0) {
                if (direction === relationship.direction) {
                    return true;
                }
            }
            if (INVERSE_RELATIONSHIPS.indexOf(relationship.type) >= 0) {
                if (direction !== relationship.direction) {
                    return true;
                }
            }
            return false;
        }),

        ...(outgoingRelationships?.relationships || []).filter((relationship) => {
            if (FORWARD_RELATIONSHIPS.indexOf(relationship.type) >= 0) {
                if (direction === relationship.direction) {
                    return true;
                }
            }
            if (INVERSE_RELATIONSHIPS.indexOf(relationship.type) >= 0) {
                if (direction !== relationship.direction) {
                    return true;
                }
            }
            return false;
        }),
    ].map((relationship) => ({ entity: relationship.entity, type: relationship.entity.type } as EntityAndType));
}
