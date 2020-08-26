import { module, test } from 'qunit';
import { setupRenderingTest } from 'ember-qunit';
import { render, findAll } from '@ember/test-helpers';
import hbs from 'htmlbars-inline-precompile';
import Component from '@ember/component';
import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';

module('Integration | Component | avatar/generic-wrapper', function(hooks) {
  setupRenderingTest(hooks);

  test('it renders and behaves as expected', async function(assert) {
    const sampleEntity = new PersonEntity('urn:li:corpuser:pikachu');
    const sampleOptions = { component: 'avatar1' };

    this.owner.register(
      'component:avatar1',
      Component.extend({ layout: hbs`<div class="avatar-1">{{@avatar.username}}</div>` })
    );
    this.setProperties({ entity: sampleEntity, options: sampleOptions });

    await render(hbs`<Avatar::GenericWrapper @value={{this.entity}} @options={{this.options}} />`);
    assert.dom('.avatar-1').exists('Renders an avatar subcomponent as expected');
    assert.dom('.avatar-1').hasText('pikachu', 'Was given expected avatar object');

    const sampleEntityList = [sampleEntity, sampleEntity, sampleEntity];
    this.set('entityList', sampleEntityList);

    await render(hbs`<Avatar::GenericWrapper @value={{this.entityList}} @options={{this.options}} />`);
    assert.equal(findAll('.avatar-1').length, 3, 'Renders a list of subcomponents when given multiple entities');

    this.owner.register(
      'component:avatar2',
      Component.extend({ layout: hbs`<div class="avatar-2">{{@avatars.length}}</div>` })
    );

    const sampleAsListOptions = { component: 'avatar2', avatarsAsList: true };
    this.set('listOptions', sampleAsListOptions);

    await render(hbs`<Avatar::GenericWrapper @value={{this.entityList}} @options={{this.listOptions}} />`);
    assert.equal(findAll('.avatar-2').length, 1, 'Renders only one subcomponent when avatarsAsList is true');
    assert.dom('.avatar-2').hasText('3', 'Subcomponent is given the proper avatar array');
  });
});
