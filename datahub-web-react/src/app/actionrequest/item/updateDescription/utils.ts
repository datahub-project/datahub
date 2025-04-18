import DOMPurify from 'dompurify';

import { tryExtractSubResourceDescription } from '@src/app/entityV2/shared/utils';
import { useEntityRegistry } from '@src/app/useEntityRegistry';
import { ActionRequest, EntityType, SubResourceType } from '@src/types.generated';

export const useGetEntityNameFromActionRequest = (actionRequest: ActionRequest) => {
    const entityRegistry = useEntityRegistry();

    let entityName = '';
    if (actionRequest.entity) {
        const type =
            actionRequest.subResourceType === SubResourceType.DatasetField
                ? EntityType.SchemaField
                : actionRequest.entity.type;
        entityName = entityRegistry.getEntityName(type) || '';
    }
    return entityName;
};

export const useGetDescriptionDiffFromActionRequest = (actionRequest: ActionRequest) => {
    const newDescription = DOMPurify.sanitize(actionRequest.params?.updateDescriptionProposal?.description || '');
    const entityDescription =
        (actionRequest.entity as any)?.editableProperties?.description ||
        (actionRequest.entity as any)?.properties?.description;
    const subResourceDescription =
        actionRequest.subResource &&
        actionRequest.entity &&
        tryExtractSubResourceDescription(actionRequest.entity, actionRequest.subResource);
    const oldDescription = DOMPurify.sanitize(
        (actionRequest.subResource ? subResourceDescription : entityDescription) || '',
    );
    return { oldDescription, newDescription };
};
