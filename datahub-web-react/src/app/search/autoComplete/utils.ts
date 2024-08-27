import { Entity } from '../../../types.generated';
import EntityRegistry from '../../entity/EntityRegistry';

export function getAutoCompleteEntityText(displayName: string, query: string) {
    const isPrefixMatch = displayName.toLowerCase().startsWith(query.toLowerCase());
    const matchedText = isPrefixMatch ? displayName.substring(0, query.length) : '';
    const unmatchedText = isPrefixMatch ? displayName.substring(query.length, displayName.length) : displayName;

    return { matchedText, unmatchedText };
}

export function getShouldDisplayTooltip(entity: Entity, entityRegistry: EntityRegistry) {
    const hasDatasetStats = (entity as any).lastProfile?.length > 0 || (entity as any).statsSummary?.length > 0;
    const genericEntityProps = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const parentContainers = genericEntityProps?.parentContainers?.containers;
    const hasMoreThanTwoContainers = parentContainers && parentContainers.length > 2;

    return hasDatasetStats || !!hasMoreThanTwoContainers;
}
