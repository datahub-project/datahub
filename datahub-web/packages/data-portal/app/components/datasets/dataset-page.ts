import Component from '@glimmer/component';

interface IDatasetsDatasetPageArgs {
  /**
   * Urn of the dataset to render
   */
  urn: string;

  /**
   * Tab selected in the dataset page
   */
  tabSelected: string;

  /**
   * username of current user
   */
  userName: string;

  /**
   * URNs that user wants to request JIT ACLS
   */
  requestJitUrns?: string;

  /**
   * Action to invoke when adding a jit acl urn
   */
  addRequestJitUrn: (urn: string) => void;

  /**
   * Action to invoke when removing a jit acl urn
   */
  removeRequestJitUrn: (urn: string) => void;

  /**
   * Action to invoke when reseting jit acl urns
   */
  resetRequestJitUrns: () => void;
}

/**
 * Dataset Page renders a full dataset page with all its tabs and content. In the future this should be
 * replaced by generic entity page + configuration.
 */
export default class DatasetsDatasetPage extends Component<IDatasetsDatasetPageArgs> {}
