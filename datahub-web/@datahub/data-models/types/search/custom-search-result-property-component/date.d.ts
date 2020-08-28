/**
 * Interface for component that renders a formatted date
 */
export interface ICustomSearchResultPropertyComponentDate {
  name: 'search/custom-search-result-property-component/date';
  options: {
    // moment format
    format: string;
    // multiplies value by 1000
    inSeconds?: boolean;
  };
}
