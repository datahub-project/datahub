import Component from '@glimmer/component';

// Styling class name for template
export const baseClass = 'read-only-email-content';

interface IReadOnlyEmailContentArgs {
  // Category that the email corresponds to
  type: Com.Linkedin.DataConstructChangeManagement.Category;
  // Subject of the email
  subject: string;
  // Core content of the email
  content: string;
  // Optional styling param that maybe injected in
  stylingClass?: string;
}

/**
 * Simple presentational component that is meant to display the contents of a email notification
 */
export default class ReadOnlyEmailContent extends Component<IReadOnlyEmailContentArgs> {
  // Styling class that defaults to `baseClass` , optionally might be an input to the component
  baseClass = this.args.stylingClass || baseClass;
}
