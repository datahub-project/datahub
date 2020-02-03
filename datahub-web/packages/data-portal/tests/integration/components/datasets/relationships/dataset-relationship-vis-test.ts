import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { getTextNoSpaces, querySelector } from '@datahub/utils/test-helpers/dom-helpers';
import { TestContext } from 'ember-test-helpers';
import { IDatasetLineage } from 'wherehows-web/typings/api/datasets/relationships';
import { INode, IEdge } from 'wherehows-web/utils/graph-db';
import { createGraph } from 'wherehows-web/tests/helpers/graph-db';
import { setProperties } from '@ember/object';
import { IDatasetEntity } from '@datahub/metadata-types/types/entity/dataset/dataset-entity';

type MyTest = TestContext & {
  nodes: Array<INode<IDatasetLineage>>;
  edges: Array<IEdge<IDatasetLineage>>;
  toggleNode: () => void;
};

module('Integration | Component | datasets/relationships/dataset-relationship-vis', function(hooks): void {
  setupRenderingTest(hooks);

  test('it renders', async function(this: MyTest, assert): Promise<void> {
    const { graphDb } = createGraph<IDatasetLineage>((id: number) => ({
      actor: `actor ${id}`,
      // For testing purposes assuming the incorrect type assertion
      // eslint-disable-next-line @typescript-eslint/no-object-literal-type-assertion
      dataset: {
        nativeName: `dataset ${id}`
      } as IDatasetEntity,
      type: `type ${id}`
    }));

    setProperties(this, {
      nodes: graphDb.nodes,
      edges: graphDb.edges,
      toggleNode: (): undefined => undefined
    });

    await render(hbs`{{datasets/relationships/dataset-relationship-vis
      nodes=nodes
      edges=edges
      toggleNode=toggleNode
    }}`);

    // Canvas can't be queried so just checking if canvas is there, assuming vis is working
    assert.ok(querySelector(this, 'canvas'), 'Canvas exists');
    assert.equal(getTextNoSpaces(this), 'LegendUnselectedParent/ChildnotloadedSelectedNode');
  });
});
