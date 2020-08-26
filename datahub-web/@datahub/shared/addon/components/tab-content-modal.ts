import Component from '@glimmer/component';
import { noop } from 'lodash';
import { ITabProperties } from '@datahub/data-models/types/entity/rendering/entity-render-props';

interface ITabContentModalModalArgs {
  /**
   * The title displayed in the header of the modal
   */
  title: string;

  /**
   * Action triggered when closing the modal
   */
  onCloseTabContentModal?: () => void;

  /**
   * Action triggered when selecting a different tab in the modal
   */
  tabSelectionDidChange?: (tab: string) => void;

  /**
   * An array of tab properties so the modal knows what the current tab will render
   */
  tabs: Array<ITabProperties>;

  /**
   * The current tab that the modal is displaying content for
   */
  currentTab: string;
}

export const modalClass = 'tab-content-modal';

/**
 * The Tab Content Modal is a modal with tab states, where the content displayed depends on the current tab selected.
 * This component is agnostic to the content that is rendered, and only renders off the tab properties.
 *
 * If the tab property do not contain sufficient attributes for the rendering of the desired content, this component
 * yields out a contextual component so this component does not get passed business logic specifics.
 * Example:
 *
 * <TabContentModal
 *  @tabs={{this.tabs}}
 *  @currentTab={{this.tabSelected}}
 *  @tabSelectionDidChange={{fn this.tabSelectionDidChange}}
 *  as |Modal|
 * >
 *   <Modal.content />
 * </TabContentModal>
 */
export default class TabContentModalModal extends Component<ITabContentModalModalArgs> {
  /**
   * Declared for convenient access in the template
   */
  modalClass = modalClass;

  /**
   * Action triggered when closing the modal
   */
  onCloseTabContentModal: () => void = this.args.onCloseTabContentModal || noop;

  /**
   * Action triggered when selecting a different tab in the modal
   */
  tabSelectionDidChange: (tab: string) => void = this.args.tabSelectionDidChange || noop;
}
