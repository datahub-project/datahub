import { GenericEntityProperties } from '@app/entity/shared/types';
import { Entity } from '@types';

type GetContextPathInput = Pick<
    GenericEntityProperties,
    'parent' | 'parentContainers' | 'parentDomains' | 'parentNodes'
>;

export function getContextPath(entityData: GetContextPathInput | null): Entity[] {
    const containerPath =
        entityData?.parentContainers?.containers ||
        entityData?.parentDomains?.domains ||
        entityData?.parentNodes?.nodes ||
        [];
    const parentPath: Entity[] = entityData?.parent ? [entityData.parent as Entity] : [];
    return containerPath.length ? containerPath : parentPath;
}
