import GraphDb, { INode } from 'wherehows-web/utils/graph-db';

export const createGraph = <T>(
  factory: (id: number) => T
): {
  graphDb: GraphDb<T>;
  node0: INode<T>;
  node1: INode<T>;
  node2: INode<T>;
  node3: INode<T>;
  node4: INode<T>;
  node5: INode<T>;
  node6: INode<T>;
  node7: INode<T>;
  node8: INode<T>;
} => {
  const graphDb: GraphDb<T> = GraphDb.create() as GraphDb<T>;
  const node0 = graphDb.addNode(factory(1));
  const node1 = graphDb.addNode(factory(2), node0);
  const node2 = graphDb.addNode(factory(3), node0, true);
  const node3 = graphDb.addNode(factory(4), node2, true);
  const node4 = graphDb.addNode(factory(5), node3, true);
  const node5 = graphDb.addNode(factory(6), node1);
  const node6 = graphDb.addNode(factory(7), node0);
  const node7 = graphDb.addNode(factory(8), node5);
  const node8 = graphDb.addNode(factory(9), node6);

  return {
    graphDb,
    node0,
    node1,
    node2,
    node3,
    node4,
    node5,
    node6,
    node7,
    node8
  };
};
