import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { hdfsUrn } from 'wherehows-web/mirage/fixtures/urn';
import sinon from 'sinon';
import { waitUntil, find } from 'ember-native-dom-helpers';
import { DatasetPlatform, PurgePolicy } from 'wherehows-web/constants';

moduleForComponent(
  'datasets/containers/upstream-dataset',
  'Integration | Component | datasets/containers/upstream dataset',
  {
    integration: true,

    beforeEach() {
      this.server = sinon.createFakeServer();
      this.server.respondImmediately = true;
    },

    afterEach() {
      this.server.restore();
    }
  }
);

test('it renders', async function(assert) {
  assert.expect(1);
  const upstreamElement = '.upstream-dataset';

  this.set('urn', hdfsUrn);
  this.set('platform', DatasetPlatform.HDFS);

  this.server.respondWith(/\/api\/v2\/datasets.*\/upstreams/, function(xhr) {
    xhr.respond(
      200,
      { 'Content-Type': 'application/json' },
      JSON.stringify([
        {
          nativeName: 'A nativeName',
          platform: DatasetPlatform.HDFS,
          uri: hdfsUrn
        }
      ])
    );
  });

  this.server.respondWith(/\/api\/v2\/datasets.*\/compliance/, function(xhr) {
    xhr.respond(
      200,
      { 'Content-Type': 'application/json' },
      JSON.stringify({
        datasetUrn: hdfsUrn
      })
    );
  });

  const retentionPolicy = {
    datasetUrn: hdfsUrn,
    purgeType: PurgePolicy.AutoPurge,
    purgeNote: null
  };

  this.server.respondWith('GET', /\/api\/v2\/datasets.*\/retention/, [
    200,
    { 'Content-Type': 'application/json' },
    JSON.stringify({ retentionPolicy })
  ]);

  this.server.respondWith('POST', /\/api\/v2\/datasets.*\/retention/, [
    200,
    { 'Content-Type': 'application/json' },
    JSON.stringify({ retentionPolicy })
  ]);

  this.server.respondWith(/\/api\/v2\/list.*\/platforms/, function(xhr) {
    xhr.respond(
      200,
      { 'Content-Type': 'application/json' },
      JSON.stringify({
        platforms: [
          {
            name: DatasetPlatform.HDFS,
            supportedPurgePolicies: [PurgePolicy.AutoLimitedRetention, PurgePolicy.AutoPurge]
          }
        ]
      })
    );
  });

  this.render(hbs`{{datasets/containers/upstream-dataset urn=urn platform=platform}}`);

  await waitUntil(() => find(upstreamElement));

  assert.equal(find(upstreamElement).textContent.trim(), 'A nativeName');
});
