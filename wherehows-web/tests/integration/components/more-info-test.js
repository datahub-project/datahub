import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';
import { click } from 'ember-native-dom-helpers';

const elementQuery = '.more-info';

moduleForComponent('more-info', 'Integration | Component | more info', {
  integration: true
});

test('it renders', function(assert) {
  this.render(hbs`{{more-info}}`);
  const element = document.querySelector(elementQuery);

  assert.ok(element, 'component is rendered in the DOM');
  assert.equal(
    element.querySelector('a').getAttribute('target'),
    '_blank',
    'sets the default target attribute when none is provided'
  );
  assert.equal(
    element.querySelector('a').getAttribute('href'),
    '#',
    'it sets a default href attribute when none is provided'
  );
});

test('MoreInfo', function(assert) {
  const externalUrl = 'https://www.linkedin.com';
  const target = '_self';

  this.set('href', externalUrl);
  this.set('target', target);

  this.render(hbs`{{more-info target=target link=href}}`);
  const element = document.querySelector(elementQuery);

  assert.equal(
    element.querySelector('a').getAttribute('target'),
    target,
    'it sets the passed in target attribute when on is provided'
  );
  assert.equal(
    element.querySelector('a').getAttribute('href'),
    externalUrl,
    'it sets the passed href attribute when a value is provided'
  );
});
