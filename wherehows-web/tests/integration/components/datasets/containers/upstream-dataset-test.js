import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { startMirage } from 'wherehows-web/initializers/ember-cli-mirage';
import { waitUntil, find } from 'ember-native-dom-helpers';
import { DatasetPlatform, PurgePolicy } from 'wherehows-web/constants';
import { hdfsUrn } from 'wherehows-web/mirage/fixtures/urn';

moduleForComponent(
  'datasets/containers/upstream-dataset',
  'Integration | Component | datasets/containers/upstream dataset',
  {
    integration: true,

    beforeEach() {
      this.server = startMirage();
    },

    afterEach() {
      this.server.shutdown();
    }
  }
);

test('it renders', async function(assert) {
  assert.expect(1);
  const { server } = this;
  const { nativeName, platform, uri } = server.create('datasetView');
  server.createList('platform', 4);

  const upstreamElement = '.upstream-dataset';

  this.set('urn', uri);
  this.set('platform', platform);

  this.render(hbs`{{datasets/containers/upstream-dataset urn=urn platform=platform}}`);

  await waitUntil(() => find(upstreamElement));

  assert.equal(find(upstreamElement).textContent.trim(), nativeName);
});
