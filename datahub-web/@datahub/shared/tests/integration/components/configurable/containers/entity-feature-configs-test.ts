import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, settled } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import setupMirage from 'ember-cli-mirage/test-support/setup-mirage';
import { MirageTestContext } from '@datahub/utils/types/vendor/ember-cli-mirage/mirage-tests';
import setup from '@datahub/shared/mirage-addon/scenarios/entity-config';
import { IBaseEntity } from '@datahub/metadata-types/types/entity';
import { testUrn, testTarget } from '@datahub/shared/mirage-addon/test-helpers/entity-configs';
import { baseClass } from '@datahub/shared/components/entity-alert-banner';

module('Integration | Component | configurable/containers/entity-feature-configs', function(hooks): void {
  setupRenderingTest(hooks);
  setupMirage(hooks);

  test('Correctly renders configurable feature based on configs fetched', async function(this: MirageTestContext, assert): Promise<
    void
  > {
    setup(this.server);

    const testEntity: IBaseEntity = {
      urn: testUrn,
      removed: false
    };

    const configurableFeatureClassSelector = `.${baseClass}`;

    await render(hbs`
    <Configurable::Containers::EntityFeatureConfigs
    @entity={{this.entity}}
    @targetFeature={{this.targetFeature}}
    >
    <EntityAlertBanner />
    </Configurable::Containers::EntityFeatureConfigs>
    `);

    // TODO META-11247: Fix issue with mirage not importing model into data portal
    // assert.dom(configurableFeatureClassSelector).doesNotExist();

    this.setProperties({
      entity: testEntity,
      targetFeature: testTarget
    });

    await settled();

    assert.dom(configurableFeatureClassSelector).exists();
  });
});
