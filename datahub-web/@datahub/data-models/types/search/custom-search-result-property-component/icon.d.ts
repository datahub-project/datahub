/**
 * Interface for component that renders an icon
 */
export interface ICustomSearchResultPropertyComponentIcon {
  name: 'search/custom-search-result-property-component/icon';
  options: {
    // Name of the icon
    iconName: string;
    // Meaning of the icon readable by humans
    hoverText: string;
  };
}
