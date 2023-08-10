import { Entity, MatchedField } from '../../../types.generated';
import { CombinedEntityResult, combineSiblingEntities } from '../../entity/shared/siblingUtils';

export type CombinedSearchResult = CombinedEntityResult & {
    matchedFields: Array<MatchedField>;
};

export function combineSiblingsInSearchResults(
    input:
        | Array<{
              entity: Entity;
              matchedFields: Array<MatchedField>;
          }>
        | undefined,
): Array<CombinedSearchResult> {
    return combineSiblingEntities(input?.map((value) => value.entity)).map((combinedResult) => ({
        ...combinedResult,
        matchedFields: [],
    }));
}
