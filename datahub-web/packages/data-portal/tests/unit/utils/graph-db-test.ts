import { module, test } from 'qunit';
import { createGraph } from 'wherehows-web/tests/helpers/graph-db';
import { set } from '@ember/object';
import GraphDb, { INode } from 'wherehows-web/utils/graph-db';

interface IMockPayload {
  someField1: string;
}

const createGraphWithMockPayload = (): {
  graphDb: GraphDb<IMockPayload>;
  node0: INode<IMockPayload>;
  node1: INode<IMockPayload>;
  node2: INode<IMockPayload>;
  node3: INode<IMockPayload>;
  node4: INode<IMockPayload>;
  node5: INode<IMockPayload>;
  node6: INode<IMockPayload>;
  node7: INode<IMockPayload>;
  node8: INode<IMockPayload>;
} =>
  createGraph<IMockPayload>((id: number) => ({
    someField1: `node ${id - 1}`
  }));

module('Unit | Utility | GraphDb', function() {
  test('addNode works as expected', function(assert) {
    const { graphDb, node0, node1, node2 } = createGraphWithMockPayload();

    assert.equal(node0.id, 1, 'Returned node should have id');
    assert.equal(node0.level, 0, 'Returned node should have level 0');
    assert.equal(node1.id, 2, 'Returned node should have id');
    assert.equal(node1.level, 1, 'Returned node should have level 1');
    assert.equal(node2.id, 3, 'Returned node should have id');
    assert.equal(node2.level, -1, 'Returned node should have level -1');
    assert.deepEqual(graphDb.edgesByTo[node0.id][0], { from: 2, to: 1 }, 'Edge should be created');
    assert.deepEqual(graphDb.edgesByFrom[node0.id][0], { from: 1, to: 3 }, 'Edge should be created');
  });

  test('uniqueIndexes and findNode works as expected', function(assert) {
    const { graphDb, node0 } = createGraphWithMockPayload();
    set(graphDb, 'uniqueKeys', ['someField1']);

    assert.equal(graphDb.findNode({ someField1: 'node 0' }), node0, 'Index works');

    const shouldBeNode0 = graphDb.addNode({
      someField1: 'node 0'
    });

    assert.equal(shouldBeNode0, node0, 'Adding same node wont create a new one');
  });

  test('minLevel', function(assert) {
    const { graphDb } = createGraphWithMockPayload();
    assert.equal(graphDb.minLevel, -3, 'min level should match');
  });

  test('getHierarchyNodes', function(assert) {
    const { graphDb, node5 } = createGraphWithMockPayload();
    assert.deepEqual(
      graphDb.getHierarchyNodes(node5),
      [
        {
          id: 6,
          level: 2,
          payload: {
            someField1: 'node 5'
          }
        },
        {
          id: 2,
          level: 1,
          payload: {
            someField1: 'node 1'
          }
        },
        {
          id: 1,
          level: 0,
          payload: {
            someField1: 'node 0'
          }
        }
      ],
      'hierachy should match'
    );

    assert.deepEqual(
      graphDb.getHierarchyNodes(node5, false),
      [
        {
          id: 6,
          level: 2,
          payload: {
            someField1: 'node 5'
          }
        },
        {
          id: 8,
          level: 3,
          payload: {
            someField1: 'node 7'
          }
        }
      ],
      'hierachy should match'
    );
  });

  test('toggle', function(assert) {
    const { graphDb, node0, node1, node2, node3, node4, node5, node6, node7, node8 } = createGraphWithMockPayload();
    const test1 = 'Parent Selection';
    graphDb.toggle(node7.id);
    assert.ok(graphDb.nodesById[node0.id].selected, `[${test1}]: select node 0`);
    assert.ok(graphDb.nodesById[node1.id].selected, `[${test1}]: select node 1`);
    assert.notOk(graphDb.nodesById[node2.id].selected, `[${test1}]: unselect node 2`);
    assert.notOk(graphDb.nodesById[node3.id].selected, `[${test1}]: unselect node 3`);
    assert.notOk(graphDb.nodesById[node4.id].selected, `[${test1}]: unselect node 4`);
    assert.ok(graphDb.nodesById[node5.id].selected, `[${test1}]: select node 5`);
    assert.notOk(graphDb.nodesById[node6.id].selected, `[${test1}]: unselect node 6`);
    assert.ok(graphDb.nodesById[node7.id].selected, `[${test1}]: select node 7`);
    assert.notOk(graphDb.nodesById[node8.id].selected, `[${test1}]: unselect node 8`);

    const test2 = 'Node deselection';
    graphDb.toggle(node5.id);
    assert.ok(graphDb.nodesById[node0.id].selected, `[${test2}]: select node 0`);
    assert.ok(graphDb.nodesById[node1.id].selected, `[${test2}]: select node 1`);
    assert.notOk(graphDb.nodesById[node2.id].selected, `[${test2}]: unselect node 2`);
    assert.notOk(graphDb.nodesById[node3.id].selected, `[${test2}]: unselect node 3`);
    assert.notOk(graphDb.nodesById[node4.id].selected, `[${test2}]: unselect node 4`);
    assert.notOk(graphDb.nodesById[node5.id].selected, `[${test2}]: unselect node 5`);
    assert.notOk(graphDb.nodesById[node6.id].selected, `[${test2}]: unselect node 6`);
    assert.notOk(graphDb.nodesById[node7.id].selected, `[${test2}]: unselect node 7`);
    assert.notOk(graphDb.nodesById[node8.id].selected, `[${test2}]: unselect node 8`);

    const test3 = 'Different branch selection';
    graphDb.toggle(node8.id);
    assert.ok(graphDb.nodesById[node0.id].selected, `[${test3}]: select node 0`);
    assert.notOk(graphDb.nodesById[node1.id].selected, `[${test3}]: unselect node 1`);
    assert.notOk(graphDb.nodesById[node2.id].selected, `[${test3}]: unselect node 2`);
    assert.notOk(graphDb.nodesById[node3.id].selected, `[${test3}]: unselect node 3`);
    assert.notOk(graphDb.nodesById[node4.id].selected, `[${test3}]: unselect node 4`);
    assert.notOk(graphDb.nodesById[node5.id].selected, `[${test3}]: unselect node 5`);
    assert.ok(graphDb.nodesById[node6.id].selected, `[${test3}]: select node 6`);
    assert.notOk(graphDb.nodesById[node7.id].selected, `[${test3}]: unselect node 7`);
    assert.ok(graphDb.nodesById[node8.id].selected, `[${test3}]: select node 8`);

    const test4 = 'Different branch selection upstream';
    graphDb.toggle(node4.id);
    assert.ok(graphDb.nodesById[node0.id].selected, `[${test4}]: select node 0`);
    assert.notOk(graphDb.nodesById[node1.id].selected, `[${test4}]: unselect node 1`);
    assert.ok(graphDb.nodesById[node2.id].selected, `[${test4}]: select node 2`);
    assert.ok(graphDb.nodesById[node3.id].selected, `[${test4}]: select node 3`);
    assert.ok(graphDb.nodesById[node4.id].selected, `[${test4}]: select node 4`);
    assert.notOk(graphDb.nodesById[node5.id].selected, `[${test4}]: unselect node 5`);
    assert.ok(graphDb.nodesById[node6.id].selected, `[${test4}]: select node 6`);
    assert.notOk(graphDb.nodesById[node7.id].selected, `[${test4}]: unselect node 7`);
    assert.ok(graphDb.nodesById[node8.id].selected, `[${test4}]: select node 8`);

    const test5 = 'Deselect root node';
    graphDb.toggle(node0.id);
    assert.notOk(graphDb.nodesById[node0.id].selected, `[${test5}]: unselect node 0`);
    assert.notOk(graphDb.nodesById[node1.id].selected, `[${test5}]: unselect node 1`);
    assert.notOk(graphDb.nodesById[node2.id].selected, `[${test5}]: unselect node 2`);
    assert.notOk(graphDb.nodesById[node3.id].selected, `[${test5}]: unselect node 3`);
    assert.notOk(graphDb.nodesById[node4.id].selected, `[${test5}]: unselect node 4`);
    assert.notOk(graphDb.nodesById[node5.id].selected, `[${test5}]: unselect node 5`);
    assert.notOk(graphDb.nodesById[node6.id].selected, `[${test5}]: unselect node 6`);
    assert.notOk(graphDb.nodesById[node7.id].selected, `[${test5}]: unselect node 7`);
    assert.notOk(graphDb.nodesById[node8.id].selected, `[${test5}]: unselect node 8`);
  });
});
