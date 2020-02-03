import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { createGraph } from 'wherehows-web/tests/helpers/graph-db';
import { setProperties } from '@ember/object';
import { TestContext } from 'ember-test-helpers';
import { IDatasetLineage } from 'wherehows-web/typings/api/datasets/relationships';
import { getTextNoSpaces } from '@datahub/utils/test-helpers/dom-helpers';
import { IEdge, INode } from 'wherehows-web/utils/graph-db';
import { IDatasetEntity } from '@datahub/metadata-types/types/entity/dataset/dataset-entity';

type MyTest = TestContext & {
  nodes: Array<INode<IDatasetLineage>>;
  edges: Array<IEdge<IDatasetLineage>>;
  toggleNode: () => void;
};

module('Integration | Component | datasets/relationships/dataset-relationship-levels', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(this: MyTest, assert): Promise<void> {
    const { graphDb, node0 } = createGraph<IDatasetLineage>((id: number) => ({
      actor: `actor ${id}`,
      // For testing purposes assuming the incorrect type assertion
      // eslint-disable-next-line @typescript-eslint/no-object-literal-type-assertion
      dataset: {
        nativeName: `dataset ${id}`
      } as IDatasetEntity,
      type: `type ${id}`
    }));
    graphDb.nodes.forEach(node =>
      graphDb.setNodeAttrs(node.id, {
        loaded: true
      })
    );
    graphDb.setNodeAttrs(node0.id, {
      selected: true
    });

    setProperties(this, {
      nodes: graphDb.nodes,
      edges: graphDb.edges,
      toggleNode: (): undefined => undefined
    });

    await render(hbs`{{datasets/relationships/dataset-relationship-levels
      nodes=nodes
      edges=edges
      toggleNode=toggleNode
    }}`);

    assert.equal(
      getTextNoSpaces(this),
      'Up1Leveldataset3Typetype3Actoractor3#Children1#Parents1CurrentDatasetdataset1Typetype1Actoractor1#Children2#Parents1Down1Leveldataset2Typetype2Actoractor2#Children1#Parents1dataset7Typetype7Actoractor7#Children1#Parents1'
    );
  });
});
