import IvyTabsTablistComponent from 'ivy-tabs/components/ivy-tabs-tablist';

export default IvyTabsTablistComponent.extend({
  /**
   * Overwrites the original ivy-tabs-tablist component's keyDown method, which lets the user
   * navigate through the tabs using the arrow keys. As we don't want that functionality, this
   * empty function replaces the event handler and does nothing instead
   */
  keyDown() {
    // Do nothing
  }
});
