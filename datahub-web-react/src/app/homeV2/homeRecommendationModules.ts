import { PageTemplateFragment } from '@graphql/template.generated';
import { DataHubPageModuleType, RecommendationModuleId } from '@types';

export const HOME_V2_RECOMMENDATION_MODULE_IDS: RecommendationModuleId[] = [
    RecommendationModuleId.Domains,
    RecommendationModuleId.Platforms,
    RecommendationModuleId.TopTerms,
];

/**
 * `limit` caps how many recommendation *modules* GMS returns, not the content inside them, and it
 * is applied after ranking rather than per requested module. It must stay above the number of
 * HOME candidate sources and independent of the requested `modules`: a GMS that does not honor
 * `requestContext.modules` returns its own ranked order (Platforms before Domains), so a limit
 * equal to the number of requested modules truncates away the modules the page renders.
 */
export const HOME_RECOMMENDATION_MODULE_LIMIT = 10;

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
