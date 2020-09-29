import Component from '@glimmer/component';

const baseClass = 'dh-info-panel';

/**
 * Simple information panel that yields title, description and action and has an icon
 */
export default class DhInfoPanel extends Component<{
  // Icon you would like to show as consumed by svg-jar
  icon?: string;
  // Css class for the component
  class?: string;
}> {
  baseClass = baseClass;
}
