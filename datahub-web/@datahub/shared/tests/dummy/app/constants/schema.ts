type Graph = Com.Linkedin.Metadata.Graph.Graph;

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
