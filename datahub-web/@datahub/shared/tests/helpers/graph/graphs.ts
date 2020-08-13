type Graph = Com.Linkedin.Metadata.Graph.Graph;
type Node = Com.Linkedin.Metadata.Graph.Node;
type Edge = Com.Linkedin.Metadata.Graph.Edge;

export const simpleGraph: Graph = {
  nodes: [
    {
      id: 'id1',
      displayName: 'displayName1',
      attributes: [
        {
          name: 'attribute',
          type: 'string'
        },
        {
          name: 'reference',
          type: 'DisplayName2',
          reference: 'id2'
        }
      ]
    },
    {
      id: 'id2',
      displayName: 'displayName2',
      attributes: [
        {
          name: 'attribute',
          type: 'string'
        }
      ]
    }
  ],
  rootNode: 'id1'
};

const nodes: Array<Node> = [
  { id: '0' },
  { id: '1' },
  { id: '2' },
  { id: '3', entityUrn: 'someUrn' },
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
  {
    fromNode: nodes[4].id,
    toNode: nodes[5].id
  },
  {
    fromNode: nodes[3].id,
    toNode: nodes[4].id
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

export const lineageGraph: Graph = {
  nodes,
  edges,
  rootNode: nodes[3].id
};
