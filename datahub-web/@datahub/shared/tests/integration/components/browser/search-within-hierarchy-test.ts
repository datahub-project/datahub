import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import { TestContext } from 'ember-test-helpers';
import EmberRouter from '@ember/routing/router';
import { getContext } from '@ember/test-helpers';
import { MockEntity } from '@datahub/data-models/entity/mock/mock-entity';

module('Integration | Component | browser/search-within-hierarchy', function(hooks): void {
  setupRenderingTest(hooks);

  const componentRef = '.data-search-hierarchy';
  const entityType = MockEntity.displayName;
  let count = 0;
  let segments: Array<string> = [];

  hooks.beforeEach(function(this: TestContext) {
    count = 0;
    segments = [];
    this.setProperties({
      count,
      entityType,
      segments
    });

    // Stubbing route service is require to properly generate a url
    const { owner } = getContext() as TestContext;
    const CustomRouter = EmberRouter.extend();
    CustomRouter.map(function() {
      this.route('browsesearch', function() {
        this.route('entity', {
          path: '/:entity'
        });
      });
    });
    owner.register('router:main', CustomRouter);
    owner.lookup('router:main').setupRouter();
  });

  test('Component rendering', async function(assert): Promise<void> {
    await render(hbs`{{browser/search-within-hierarchy}}`);

    assert.dom(componentRef).doesNotExist();

    // won't render with count at 0
    await render(
      hbs`{{browser/search-within-hierarchy count=this.count entityType=this.entityType segments=this.segments}}`
    );

    assert.dom(componentRef).doesNotExist();

    count = 20;
    this.set('count', count);

    // Render with count at 20 and no segments
    await render(
      hbs`{{browser/search-within-hierarchy count=this.count entityType=this.entityType segments=this.segments}}`
    );

    assert.dom(componentRef).doesNotExist();

    // Render with count at 20 and segments greater than 0
    segments = ['testSegment'];
    this.set('segments', segments);

    await render(
      hbs`{{browser/search-within-hierarchy count=this.count entityType=this.entityType segments=this.segments}}`
    );

    assert.dom(componentRef).matchesText(`View ${count} ${entityType}`);
    assert.dom(componentRef).hasAttribute('href', `#/browsesearch/${entityType}?page=1&path=testSegment`);
  });
});
