import Component from '@ember/component';
import { tagName } from '@ember-decorators/component';
import { set } from '@ember/object';

/**
 * This component will lazy load when demanded its content. Lazy load means that it will not yield
 * until currentTab === id. Once that happens, content won't be deleted or rerendered
 *
 * Params:
 *  lazyLoad: boolean ()
 *  currentTab: string (actual string value of the selected current tab's id)
 *  id: string (this tab id)
 *
 */
@tagName('')
export default class NachoTabPager extends Component {
  /**
   * if you want lazy load behavior
   */
  lazyLoad: boolean;

  /**
   * actual string value of the selected current tab's id
   */
  currentTab: string;

  /**
   * this tab id
   */
  id: string;

  /**
   * internal flag to say whether this tab has been already rendered
   */
  cached: boolean;

  /**
   * Hook to check current tab and cache accondingly
   */
  didReceiveAttrs() {
    const { id, currentTab } = this;
    if (id === currentTab) {
      set(this, 'cached', true);
    }
  }
}
