/**
 * Interface for components that will receive 'state'
 * under a content-panel-with-toggle.
 */
export interface ISharedStateArgs<State> {
  // Current state (redux like)
  state?: State;
  // State before current if available
  lastState?: State;
  // Trigger when a state change is needed
  onStateChanged: (state: State) => {};
}
