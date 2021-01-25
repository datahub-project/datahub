/* eslint-disable @typescript-eslint/interface-name-prefix */
import { IObject } from '@nacho-ui/core/types/utils/generics';

/**
 * Configs that can be passed for each header to help better render based on configurations that we
 * can customize
 */
export interface NachoTableHeaderConfig {
  // This would be the display title for each column
  title?: string;
  // This is the class that would be applied to the header cell specifically
  className?: string;
  // This is the component path to render in the cell for the header of this specific column
  component?: string;
}

/**
 * Configs that can be passed based on consumer preference for using yielded blocked components vs
 * relying on configurations. Default of all values is presumed to be false.
 */
export interface NachoTableBlockConfigs {
  header?: boolean;
  body?: boolean;
  footer?: boolean;
}

/**
 * Configs that can be passed based on consumer preference for custom rows.
 */
export interface NachoTableCustomRowConfig {
  // If we should replace the whole row with a corresponding component provided by the consumer. Takes
  // priority over other configurations
  component?: string;
  // Class to apply to the <tr> for the row in addition to the default class
  className?: string;
}

/**
 * Expected interface for a computed link given by the compute() function in displayLink for
 * NachoTableCustomColumnConfig
 */
export interface NachoTableComputedLink {
  // Href for the rendered <a> tag
  ref: string;
  // Display is optional and will default to ref if not given.
  display?: string;
}

/**
 * Configs that can be passed based on consumer preference for items that could affect the whole column
 */
export interface NachoTableCustomColumnConfig<T> {
  // Class that will apply to each table cell in each row under this column head. Should correspond to
  // a [label] in INachoTableConfigs
  className?: string;
  // Should correspond to a [label] in INachoTableConfigs, sets a custom component for each cell in every
  // row uner this column header
  component?: string;
  // A very common use case for a table cell is to display a text link. If this is the only transformation
  // we need for our cell, then having the consumer need to pass in a whole custom component is overkill.
  // Instead, they can use display link to compute the proper element attributes from their row data object
  displayLink?: {
    // Optional class name to provide to the link
    className?: string;
    // Whether the link should open in a new tab or redirect from the current one. Adds target="_blank"
    isNewTab?: boolean;
    // Function that computes the link ref and display string based on user desire. Expects a source to
    // be returned.
    compute: (rowData: T) => NachoTableComputedLink;
  };
  /**
   * Optional function that may be invoked by the custom component for example, if an action is needed to be performed
   * by a higher level component
   */
  componentAction?: Function;
}

/**
 * Filters are a common practice for tables, and these params will help the consuming application implement
 * a straightforward filter logic
 */
export interface NachoTableFilterConfig<T> {
  // Filter by key will just compare a specified key on the rowData to the filterString, whereas compare
  // will allow for more complex filter comparison functions
  filterBy: 'key' | 'compare';
  // Filter by a "search" input or dropdown options. If options is selected, then a list of options for
  // filtering should be provided
  filterMethod: 'search' | 'options';
  // A static list of options to render
  // TODO: Provide a way to get a dynamic list of options
  options?: Array<{ label: string; value: unknown }>;
  compare?: (rowData: T, filterString: string) => boolean;
}

export interface INachoTableConfigs<T> {
  headers?: Array<NachoTableHeaderConfig>;
  // Important! This is a required property that allows you to create an identifier for each column
  labels: Array<string>;
  // Import booleans for whether or not we want to yield to the given block for each
  useBlocks?: NachoTableBlockConfigs;
  customRows?: NachoTableCustomRowConfig;
  customColumns?: IObject<NachoTableCustomColumnConfig<T>>;
  filters?: IObject<NachoTableFilterConfig<T>>;
  isHidingHeader?: boolean;
}
