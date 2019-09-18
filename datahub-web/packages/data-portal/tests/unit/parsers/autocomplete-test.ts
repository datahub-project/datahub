import { module, test } from 'qunit';
import grammar from 'wherehows-web/parsers/autocomplete';
import { parse } from 'wherehows-web/utils/parsers/parser';

const tests = [
  {
    description: '1 Dataset',
    text: 'dataset1',
    result: [
      {
        dataset: 'dataset1'
      }
    ]
  },
  {
    description: '2 Datasets',
    text: 'dataset1 dataset2',
    result: [
      {
        dataset: 'dataset1'
      },
      {
        dataset: 'dataset2'
      }
    ]
  },
  {
    description: '2 Datasets with AND',
    text: 'dataset1 AND dataset2',
    result: [
      {
        dataset: 'dataset1'
      },
      'AND',
      {
        dataset: 'dataset2'
      }
    ]
  },
  {
    description: 'Spaces',
    text: '     dataset1     AND       dataset2      ',
    result: [
      {
        dataset: 'dataset1'
      },
      'AND',
      {
        dataset: 'dataset2'
      }
    ]
  },
  {
    description: 'Parenthesis1',
    text: '   (  dataset1     AND       dataset2    )  ',
    result: [
      [
        {
          dataset: 'dataset1'
        },
        'AND',
        {
          dataset: 'dataset2'
        }
      ]
    ]
  },
  {
    description: 'Parenthesis2',
    text: '   (dataset1 AND dataset2) OR (dataset3 AND dataset4) ',
    result: [
      [
        {
          dataset: 'dataset1'
        },
        'AND',
        {
          dataset: 'dataset2'
        }
      ],
      'OR',
      [
        {
          dataset: 'dataset3'
        },
        'AND',
        {
          dataset: 'dataset4'
        }
      ]
    ]
  },
  {
    description: 'Facets 1',
    text: '   (name:dataset1 AND dataset2) OR (dataset3 AND dataset4) ',
    result: [
      [
        {
          facetName: 'name',
          facetValue: 'dataset1'
        },
        'AND',
        {
          dataset: 'dataset2'
        }
      ],
      'OR',
      [
        {
          dataset: 'dataset3'
        },
        'AND',
        {
          dataset: 'dataset4'
        }
      ]
    ]
  },
  {
    description: 'Facets 2',
    text: '   (name:dataset1 AND platform:something) OR ( origin:corp AND dataset4 )',
    result: [
      [
        {
          facetName: 'name',
          facetValue: 'dataset1'
        },
        'AND',
        {
          facetName: 'platform',
          facetValue: 'something'
        }
      ],
      'OR',
      [
        {
          facetName: 'origin',
          facetValue: 'corp'
        },
        'AND',
        {
          dataset: 'dataset4'
        }
      ]
    ]
  }
];

module('Unit | Helper | Parsers', function() {
  test('Grammar works as expected', function(assert) {
    tests.forEach(myTest => {
      try {
        const parser = parse(myTest.text.trim(), grammar);
        assert.ok(true, `${myTest.description}: grammar parse correctly`);
        assert.deepEqual(parser.results[0], myTest.result, `${myTest.description}: expected tree match`);

        assert.equal(parser.results.length, 1, `${myTest.description}: no ambiguity`);
      } catch (e) {
        assert.ok(false, `${myTest.description}: ${e}`);
      }
    });
  });
});
