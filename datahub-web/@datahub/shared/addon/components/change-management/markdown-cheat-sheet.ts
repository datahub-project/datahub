import Component from '@glimmer/component';

// Styling class name for template
export const baseClass = 'markdown-cheat-sheet';

/**
 * Simple presentational component that is meant to display the contents of a email notification
 */
export default class MarkdownCheatSheet extends Component<{}> {
  // Styling class that defaults to `baseClass` , optionally might be an input to the component
  baseClass = baseClass;
}
