import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../templates/components/blank-template';
import { layout, tagName } from '@ember-decorators/component';

/**
 * The BlankTemplate component is blank, but it allows us to create multiple yields for a component
 * with custom content in each one
 */
@layout(template)
@tagName('')
export default class BlankTemplate extends Component {}
