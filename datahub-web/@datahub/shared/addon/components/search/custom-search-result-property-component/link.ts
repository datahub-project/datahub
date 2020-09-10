import Component from '@glimmer/component';
import { DataModelEntity } from '@datahub/data-models/constants/entity';
import { ICustomSearchResultPropertyComponentLink } from '@datahub/data-models/types/search/custom-search-result-property-component/link';

/**
 * Component to render external links
 */
export default class CustomSearchResultPropertyComponentSearchLink extends Component<{
  /**
   * The entity that we want to get the property from
   */
  entity?: DataModelEntity;

  /**
   * Options for this component
   */
  options?: ICustomSearchResultPropertyComponentLink['options'];
}> {}
