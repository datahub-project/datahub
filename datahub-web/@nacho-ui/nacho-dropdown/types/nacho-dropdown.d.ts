/**
 * Basic interface typing for a single dropdown option used in this addon. Label will be what appears in the
 * visual menu, while value can be any arbitrary value assigned to that label
 */
export interface INachoDropdownOption<T> {
  // Display for each option
  label: string;
  // Value that gets passed into the onchange of the list. Allows a different designation for labels (human readable
  // display that the user sees) vs identifiers attached to those labels
  value: T;
  // Whether the option is selected or not. Optional because this can be taken care of by default in the component
  isSelected?: boolean;
  // Whether or not to allow the user to select the particular option
  isDisabled?: boolean;
  // Reserved for when we want to show a category "header" for each set of options to give more content to that set
  // for the users
  isCategoryHeader?: boolean;
}

/**
 * Typing to signify a list of dropdown options
 */
export type NachoDropdownOptions<T> = Array<INachoDropdownOption<T>>;
