import {
  excludeSimpleAttributes,
  moveAttributeEdgesToEdges,
  getAllPaths,
  excludeNodes
} from '@datahub/shared/utils/graph/graph-transformations';
import { module, test } from 'qunit';
import { simpleGraph } from '../../../helpers/graph/graphs';

type Graph = Com.Linkedin.Metadata.Graph.Graph;
type Node = Com.Linkedin.Metadata.Graph.Node;
type Edge = Com.Linkedin.Metadata.Graph.Edge;

module('Unit | Utility | graph-transformations', function(_hooks): void {
  test('excludeSimpleAttributes', function(assert): void {
    const tGraph = excludeSimpleAttributes(simpleGraph);
    assert.deepEqual(tGraph, {
      nodes: [
        {
          attributes: [
            {
              name: 'reference',
              reference: 'id2',
              type: 'DisplayName2'
            }
          ],
          displayName: 'displayName1',
          id: 'id1'
        },
        {
          attributes: [],
          displayName: 'displayName2',
          id: 'id2'
        }
      ],
      rootNode: 'id1'
    });
  });

  test('moveAttributeEdgesToEdges', function(assert): void {
    const tGraph = moveAttributeEdgesToEdges(simpleGraph);
    assert.deepEqual(tGraph, {
      edges: [
        {
          attributes: [
            {
              name: 'displayName1',
              value: 'reference'
            }
          ],
          fromAttribute: 'reference',
          fromNode: 'id1',
          toNode: 'id2'
        }
      ],
      nodes: [
        {
          attributes: [
            {
              name: 'attribute',
              type: 'string'
            },
            {
              name: 'reference',
              reference: 'id2',
              type: 'DisplayName2'
            }
          ],
          displayName: 'displayName1',
          id: 'id1'
        },
        {
          attributes: [
            {
              name: 'attribute',
              type: 'string'
            }
          ],
          displayName: 'displayName2',
          id: 'id2'
        }
      ],
      rootNode: 'id1'
    });
  });

  test('getAllPaths', function(assert): void {
    const nodes: Array<Node> = [
      { id: '0' },
      { id: '1' },
      { id: '2' },
      { id: '3' },
      { id: '4' },
      { id: '5' },
      { id: '6' }
    ];
    const edges: Array<Edge> = [
      {
        fromNode: nodes[0].id,
        toNode: nodes[1].id
      },
      {
        fromNode: nodes[0].id,
        toNode: nodes[2].id
      },
      {
        fromNode: nodes[2].id,
        toNode: nodes[3].id
      },
      {
        fromNode: nodes[0].id,
        toNode: nodes[3].id
      },
      // Circular depencency test
      {
        fromNode: nodes[4].id,
        toNode: nodes[5].id
      },
      {
        fromNode: nodes[5].id,
        toNode: nodes[4].id
      },
      {
        fromNode: nodes[4].id,
        toNode: nodes[6].id
      },
      {
        fromNode: nodes[6].id,
        toNode: nodes[5].id
      }
    ];
    const graph: Graph = {
      nodes,
      edges
    };
    const singlePath = getAllPaths(graph, nodes[0].id, nodes[1].id);
    assert.deepEqual(singlePath, [
      {
        nodes: [nodes[0].id, nodes[1].id],
        edges: [edges[0]]
      }
    ]);

    const multiplePaths = getAllPaths(graph, nodes[0].id, nodes[3].id);
    assert.deepEqual(multiplePaths, [
      {
        edges: [
          {
            fromNode: nodes[0].id,
            toNode: nodes[3].id
          }
        ],
        nodes: [nodes[0].id, nodes[3].id]
      },
      {
        edges: [
          {
            fromNode: nodes[0].id,
            toNode: nodes[2].id
          },
          {
            fromNode: nodes[2].id,
            toNode: nodes[3].id
          }
        ],
        nodes: [nodes[0].id, nodes[2].id, nodes[3].id]
      }
    ]);

    const withLoop = getAllPaths(graph, nodes[4].id, nodes[5].id);
    assert.deepEqual(withLoop, [
      {
        edges: [
          {
            fromNode: nodes[4].id,
            toNode: nodes[5].id
          }
        ],
        nodes: [nodes[4].id, nodes[5].id]
      },
      {
        edges: [
          {
            fromNode: nodes[4].id,
            toNode: nodes[6].id
          },
          {
            fromNode: nodes[6].id,
            toNode: nodes[5].id
          }
        ],
        nodes: [nodes[4].id, nodes[6].id, nodes[5].id]
      }
    ]);
  });

  test('excludeNodes', function(assert): void {
    const nodes: Array<Node> = [
      { id: '0' },
      { id: '1' },
      { id: '2' },
      { id: '3' },
      { id: '4' },
      { id: '5' },
      { id: '6' }
    ];
    const edges: Array<Edge> = [
      {
        fromNode: nodes[0].id,
        toNode: nodes[1].id
      },
      {
        fromNode: nodes[0].id,
        toNode: nodes[2].id
      },
      {
        fromNode: nodes[2].id,
        toNode: nodes[0].id
      },
      {
        fromNode: nodes[2].id,
        toNode: nodes[3].id
      },
      {
        fromNode: nodes[0].id,
        toNode: nodes[3].id
      },
      {
        fromNode: nodes[0].id,
        toNode: nodes[4].id
      },
      // Circular depencency test
      {
        fromNode: nodes[4].id,
        toNode: nodes[5].id
      },
      {
        fromNode: nodes[5].id,
        toNode: nodes[4].id
      },
      {
        fromNode: nodes[4].id,
        toNode: nodes[6].id
      },
      {
        fromNode: nodes[6].id,
        toNode: nodes[5].id
      }
    ];
    //
    const graph: Graph = {
      rootNode: '0',
      nodes,
      edges
    };

    const excluded = excludeNodes(graph, '5');
    assert.deepEqual(excluded, {
      rootNode: '0',
      edges: [
        {
          fromNode: '0',
          toNode: '1'
        },
        {
          fromNode: '0',
          toNode: '2'
        },
        {
          fromNode: '2',
          toNode: '0'
        },
        {
          fromNode: '2',
          toNode: '3'
        },
        {
          fromNode: '0',
          toNode: '3'
        },
        {
          fromNode: '0',
          toNode: '4'
        },
        {
          fromNode: '4',
          toNode: '6'
        }
      ],
      nodes: [
        {
          id: '0'
        },
        {
          id: '1'
        },
        {
          id: '2'
        },
        {
          id: '3'
        },
        {
          id: '4'
        },
        {
          id: '6'
        }
      ]
    });
  });
});
