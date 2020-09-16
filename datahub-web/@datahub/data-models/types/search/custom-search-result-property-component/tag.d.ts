/**
 * Interface for component that renders a tag
 */
export interface ICustomSearchResultPropertyComponentTag {
  name: 'search/custom-search-result-property-component/tag';
  options: {
    // should be type PillState but it is not exposed in nacho-ui
    state: string;
    // should use titleize helper to format the text
    titleize?: boolean;
    // if text is used, then value should be a boolean
    text?: string;
  };
}
