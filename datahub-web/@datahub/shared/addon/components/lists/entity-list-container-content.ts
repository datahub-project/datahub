import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../templates/components/lists/entity-list-container-content';
import { layout } from '@ember-decorators/component';

/**
 * This component is the content wrapper for the entity-list-container to be used with the
 * generalized content
 */
@layout(template)
export default class EntityListContainerContent extends Component {}
