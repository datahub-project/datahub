import { PageTemplateFragment } from '@graphql/template.generated';
import { DataHubPageModuleType, RecommendationModuleId } from '@types';

export const HOME_V2_RECOMMENDATION_MODULE_IDS: RecommendationModuleId[] = [
    RecommendationModuleId.Domains,
    RecommendationModuleId.Platforms,
    RecommendationModuleId.TopTerms,
];

export function pageModuleTypeToRecommendationModuleId(
    type: DataHubPageModuleType | null | undefined,
): RecommendationModuleId | undefined {
    if (type === DataHubPageModuleType.Domains) {
        return RecommendationModuleId.Domains;
    }
    if (type === DataHubPageModuleType.Platforms) {
        return RecommendationModuleId.Platforms;
    }
    return undefined;
}

export function sortRecommendationModuleIds(moduleIds: RecommendationModuleId[]): RecommendationModuleId[] {
    return [...new Set(moduleIds)].sort((a, b) => a.localeCompare(b));
}

export function collectHomeRecommendationModuleIds(
    template: PageTemplateFragment | null | undefined,
): RecommendationModuleId[] {
    const moduleIds: RecommendationModuleId[] = [];
    template?.properties?.rows?.forEach((row) => {
        row?.modules?.forEach((module) => {
            const moduleId = pageModuleTypeToRecommendationModuleId(module?.properties?.type);
            if (moduleId) {
                moduleIds.push(moduleId);
            }
        });
    });
    return sortRecommendationModuleIds(moduleIds);
}
