/**
 * Enum of the possible types of ways a user might interact with a control that we plan to support
 * with our control interaction component
 */
export enum ControlInteractionType {
  // The user focused on a UI control.
  focus = 'FOCUS',
  // The user taps or clicks a UI control
  click = 'SHORT_PRESS',
  // The user hovers the cursor over a UI container or control
  hover = 'HOVER'
}
