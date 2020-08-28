/**
 * Interface for component that renders an external link
 */
export interface ICustomSearchResultPropertyComponentLink {
  name: 'search/custom-search-result-property-component/link';
  options: {
    /**
     * property from entity that will return a ILink
     */
    linkProperty: string;

    /**
     * Text for the button
     */
    text: string;
  };
}
