import { EntityType } from '../../../../types.generated';
import { GenericEntityProperties } from '../types';

export function isDeleteDisabled(entityType: EntityType, entityData: GenericEntityProperties | null) {
    if (entityType === EntityType.GlossaryTerm || entityType === EntityType.GlossaryNode) {
        const entityHasChildren = !!entityData?.children?.total;
        const canManageGlossaryEntity = !!entityData?.privileges?.canManageEntity;
        const canDeleteGlossaryEntity = !entityHasChildren && canManageGlossaryEntity;
        return !canDeleteGlossaryEntity;
    }
    if (entityType === EntityType.DataProduct) {
        return false; // TODO: update with permissions
    }
    return false;
}
