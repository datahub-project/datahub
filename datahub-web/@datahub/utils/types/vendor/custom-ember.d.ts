import ArrayPrototypeExtensions from '@ember/array/types/prototype-extensions';
import RouterService from '@ember/routing/router-service';
import Transition from '@ember/routing/-private/transition';

// opt-in to allow types for Ember Array Prototype extensions
declare global {
  // eslint-disable-next-line @typescript-eslint/no-empty-interface, @typescript-eslint/interface-name-prefix
  interface Array<T> extends ArrayPrototypeExtensions<T> {}
}

// TODO META-11860 Check on Ember side typings for RouterService and remove temporal fix for RouterServiceFix
/**
 * Temporal fix until ember release proper typings for routing with just params
 */
export type RouterServiceFix = RouterService & {
  transitionTo(options: { queryParams: object }): Transition;
  replaceWith(options: { queryParams: object }): Transition;
};

/**
 * Describes the interface for the callback function accepeted by onUpdateURL in an IEmberLocation instance
 * @alias {(url: string) => void}
 */
type UpdateCallback = (url: string) => void;

/**
 * Defines the interface for the EmberRouter location type
 * @export
 * @interface IEmberLocation
 */
export interface IEmberLocation {
  implementation: string;
  cancelRouterSetup?: boolean;
  getURL(): string;
  setURL(url: string): void;
  replaceURL?(url: string): void;
  onUpdateURL(callback: UpdateCallback): void;
  formatURL(url: string): string;
  detect?(): void;
  initState?(): void;
}
