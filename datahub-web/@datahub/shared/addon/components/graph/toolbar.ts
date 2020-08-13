import Component from '@glimmer/component';
import { action } from '@ember/object';
import { ISharedStateArgs } from '@datahub/shared/types/entity-page/components/entity-page-content/content-panel-with-toggle';
import { IGraphViewerState } from '@datahub/shared/types/graph/graph-viewer-state';
import Changeset from 'ember-changeset';
import { tracked } from '@glimmer/tracking';
import { IEmberSimpleDropdown } from '@datahub/utils/types/vendor/ember-simple-dropdown';

/**
 * The toolbar component for the graph viewer contains the wrapper for the buttons and triggers to
 * search and modify graph settings. We split it into its own component so that the graph-viewer
 * component can be about painting the graph and this component can be toolbar-specific logic
 */
export default class GraphViewerToolbar extends Component<ISharedStateArgs<IGraphViewerState>> {
  /**
   * Base CSS class
   */
  baseCssClass = 'graph-viewer-toolbar';

  /**
   * Change Set for settings form
   */
  @tracked
  changeSet?: Changeset<IGraphViewerState>;

  /**
   * Settings dropdown reference to allow to open and close programatically
   */
  @tracked
  dropdown?: IEmberSimpleDropdown;

  /**
   * Calculate the number of hidden nodes based on the number of nodes that
   * the graph is showing.
   *
   * @readonly
   */
  get numberOfHiddenNodes(): number {
    const { state } = this.args;
    return (state?.graph?.nodes.length || 0) - (state?.numberShowingNodes || 0);
  }

  /**
   * Once we click on search icon, toggle flag isSearching to hide/show search bar
   */
  @action
  onSearchIconClick(): void {
    const state = this.args.state || {};

    this.args.onStateChanged({
      ...state,
      isSearching: !state.isSearching
    });
  }

  /**
   * Once the dropdown trigger, we will receive the element plus the dropdown interface
   * @param _element dropdown trigger element
   * @param args Tuple containing dropdown interface
   */
  @action
  didInsertDropdownTrigger(_element: unknown, args: [IEmberSimpleDropdown]): void {
    const [dropdown] = args;
    this.dropdown = dropdown;
  }

  /**
   * If they click on the depth button, we will open the settings dropdown
   */
  @action
  openConfigurationPanel(): void {
    this.dropdown?.actions.open();
  }

  /**
   * Search nodes matching text typed by user
   * @param typeaheadText
   */
  @action
  onSearchNode(typeaheadText: string): Array<Com.Linkedin.Metadata.Graph.Node> {
    const state = this.args.state || {};
    const { graph } = state;
    const typeaheadOptions =
      (graph &&
        graph.nodes.filter(
          (node): boolean =>
            node.id.toUpperCase().indexOf(typeaheadText.toUpperCase()) >= 0 ||
            (node.displayName || '').toUpperCase().indexOf(typeaheadText.toUpperCase()) >= 0
        )) ||
      [];

    this.args.onStateChanged({
      ...state,
      typeaheadText,
      typeaheadOptions
    });

    return typeaheadOptions;
  }

  /**
   * User selects a node from the typeahead list
   * @param selectedNode node selected by the user
   */
  @action
  onTypeaheadSelect(selectedNode: Com.Linkedin.Metadata.Graph.Node): void {
    const state = this.args.state || {};
    this.args.onStateChanged({
      ...state,
      isSearching: false,
      selectedNodeId: selectedNode.id,
      typeaheadOptions: [],
      typeaheadText: ''
    });
  }

  /**
   * On dropdown open we will create a new changeset from the state.
   * Cloning the state is important as changeset will change the reference
   */
  @action
  initChangeSet(): void {
    this.changeSet = this.args.state && new Changeset({ ...this.args.state });
  }

  /**
   * When the user selects cancel we will rollback the changes and close the dropdown
   */
  @action
  onCancel(): void {
    this.changeSet?.rollback();
    this.dropdown?.actions.close();
  }

  /**
   * Toggling show connections switch
   */
  @action
  onSubmit(): void {
    const state = this.args.state || {};
    this.dropdown?.actions.close();
    this.changeSet?.save();
    this.args.onStateChanged({
      ...state,
      ...this.changeSet?.data
    });
  }
}
