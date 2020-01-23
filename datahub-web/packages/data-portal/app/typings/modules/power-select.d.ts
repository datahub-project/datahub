/**
 * Object properties in params provided by the ember-power-select component to some passed in callbacks
 */
export interface IPowerSelectAPI<T> {
  disabled: boolean; // Truthy if the component received `disabled=true`
  highlighted: T; // Contains the currently highlighted option (if any)
  isActive: boolean; // Truthy if the trigger is focused. Other subcomponents can mark it as active depending on other logic.
  isOpen: boolean; // Truthy if the dropdown is open.
  lastSearchedText: string; // Contains the text of the last finished search. In sync searches will match `searchText`. In async searches, it will match it if the current search is fulfilled
  loading: boolean; // Truthy if there is a pending promise that will update the results
  options: Array<T>; // Contains the regular array with the resolved collection of options.
  results: Array<T>; // Contains the regular array with the active set of results.
  resultsCount: number; // Contains the number of results incuding those nested/disabled
  searchText: string; // Contains the text of the current search
  selected: Array<T>; // Contains the resolved selected option (or options in multiple selects)
  uniqueId: string; // Contains the unique of this instance of EmberPowerSelect. It's of the form `ember1234`.
  actions: {
    choose(option: T): void; // Chooses the given options if it's not disabled (slight different than `select`)
    close(event?: Event): void; // Closes the select
    highlight(option: T): void; // Highlights the given option (if it's not disabled)
    open(): void; // Opens the select
    reposition(): void; // Repositions the dropdown (noop if renderInPlace=true)
    scrollTo(option: T): void; // Scrolls the given option into the viewport
    search(term: string): void; // Performs a search
    select(option: T): void; // Selects the given option (if it's not disabled)
    toggle(): void;
  };
}
