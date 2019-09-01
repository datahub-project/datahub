import { module, test } from 'qunit';
import {
  mergeFacetCountsWithSelections,
  searchResultMetasToFacetCounts
} from 'wherehows-web/utils/search/search-results';
import { IFacetsCounts } from '@datahub/data-models/types/entity/facets';
import { IAggregationMetadata } from 'wherehows-web/typings/api/search/entity';

module('Unit | Utility | search/search-results', function(): void {
  test('mergeFacetCountsWithSelections', function(assert): void {
    const searchResult: IFacetsCounts = {
      status: {
        unpublished: 3
      }
    };
    const selections: Record<string, Array<string>> = {
      status: ['published'],
      tier: ['tier1']
    };
    const expected: IFacetsCounts = {
      status: {
        unpublished: 3,
        published: 0
      },
      tier: {
        tier1: 0
      }
    };
    assert.deepEqual(mergeFacetCountsWithSelections(searchResult, selections), expected);
  });

  test('searchResultMetasToFacetCounts', function(assert): void {
    const searchResult: Array<IAggregationMetadata> = [
      {
        name: 'status',
        aggregations: {
          unpublished: 3
        }
      }
    ];
    const selections: Record<string, Array<string>> = {
      status: ['published'],
      tier: ['tier1']
    };
    const expected: IFacetsCounts = {
      status: {
        unpublished: 3,
        published: 0
      },
      tier: {
        tier1: 0
      }
    };
    assert.deepEqual(searchResultMetasToFacetCounts(searchResult, selections), expected);
  });
});
