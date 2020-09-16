import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../../../../templates/components/institutional-memory/wiki/url-list/add-dialog';
import { layout, classNames } from '@ember-decorators/component';
import { action } from '@ember/object';
import { baseTableClass } from '@datahub/shared/components/institutional-memory/wiki/url-list';
import { setProperties } from '@ember/object';
import { noop } from 'lodash';

/**
 * Creates a modal class base string for convenience
 * @type {string}
 */
const modalClass = `${baseTableClass}-modal`;
@layout(template)
@classNames(`${modalClass}__container`)
export default class WikiUrlListAddDialog extends Component {
  /**
   * Saves to component for convenient template access
   */
  baseModalClass: string = modalClass;

  /**
   * Classnames passed to the modal for the container class of the popup window
   */
  containerClassNames: Array<string> = [modalClass];

  /**
   * Passed in method to process the action triggered to save the link generated in the
   * form for this modal dialog
   */
  saveLink: (url: string, description: string) => void = noop;

  /**
   * Passed in method to process the action triggered to cancel the link generation process
   * and close the modal dialog
   */
  cancelAddLink: () => void = noop;

  /**
   * User input url for the wiki link they want to create
   */
  url = '';

  /**
   * User input description for the wiki link they want to create
   */
  description = '';

  /**
   * Set character limit for the description
   */
  descriptionCharLimit = 500;

  /**
   * User triggered confirm to save the link that they created. We only pass the params
   * up from here and allow the container to do with the information as it pleases,
   * leaving this component to only be concerned with the creation of the form
   */
  @action
  onSaveLink(): void {
    const { url, description, saveLink, descriptionCharLimit } = this;
    saveLink(url, description.slice(0, descriptionCharLimit));
  }

  /**
   * User triggered cancel action to close this dialog for the link creation.
   */
  @action
  onCancel(): void {
    setProperties(this, { url: '', description: '' });
    this.cancelAddLink();
  }
}
