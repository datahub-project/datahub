import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../../templates/components/user/profile/page-content/renderer';
import { layout, tagName } from '@ember-decorators/component';

/**
 * The purpose of the page content renderer is to allow us to create reusable blocks of code for
 * our tabs, since our tabs now can behave as navigation or be a "header" for multiple "subtab"
 * categories
 *
 * When we are dealing with a tablist, we expect to yield an array of tab items. However, we can
 * also use the renderer for a single tab
 * @example
 * ```html
 * <User::Profile::PageContent::Renderer @isTablist={{true}} @tabProperties={{something}} as |tabs|>
 *   {{#each tabs.tablist as |aTab|}}
 *     ...
 *   {{/each}}
 * </User::Profile::PageContent::Renderer>
 * ```
 *
 * @example
 * ```html
 * <User::Profile::PageContent::Renderer @isTablist={{false}} @tabProperties={{something}} as |tab|>
 *   ... Do something with {{tab.tab}}
 * </User::Profile::PageContent::Renderer>
 * ```
 */
// TODO: [META-10011] Generalize this component for future tab structure stuff
@layout(template)
@tagName('')
export default class UserProfilePageContentRenderer extends Component {}
