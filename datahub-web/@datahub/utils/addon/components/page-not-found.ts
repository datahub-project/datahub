import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../templates/components/page-not-found';
import { layout } from '@ember-decorators/component';

/**
 * Simple template only component to render the page not found for Data Hub
 */
@layout(template)
export default class PageNotFound extends Component {}
