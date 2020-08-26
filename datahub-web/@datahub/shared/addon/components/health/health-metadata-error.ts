import Component from '@glimmer/component';

interface IHealthHealthMetadataErrorArgs {
  // The error that caused this to be
  error?: string;
  // The error message from the handler to be displayed
  message?: string;
}

/**
 * Custom error component for Health Metadata error
 * @export
 * @class HealthHealthMetadataError
 * @extends {Component<IHealthHealthMetadataErrorArgs>}
 */
export default class HealthHealthMetadataError extends Component<IHealthHealthMetadataErrorArgs> {}
