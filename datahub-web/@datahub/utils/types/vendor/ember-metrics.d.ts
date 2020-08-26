declare module 'ember-metrics' {
  import Service from '@ember/service';
  import BaseAdapter from 'ember-metrics/metrics-adapters/base';

  /**
   * Describes the interface of parameters that can be supplied to the Ember Metrics activateAdapters method
   * These parameters specify the name of the adapter to activate as well as additional attributes such as the
   * activation environment
   * @export
   * @interface IAdapterOptions
   */
  export interface IAdapterOptions {
    // The name of the adapter to be activated e.g. Piwik, Google Analytics
    name: string;
    // The application environments in which this adapter should be activated, defaults to all
    environments: Array<'development' | 'production' | 'test' | 'all'>;
    // Adapter specific configuration object with attributes that will be passed to the adapter on activation
    config: object;
  }

  /**
   * Ember Metrics library's service class interface
   * @export
   * @class Metrics
   * @extends {Service}
   */
  export default class Metrics extends Service {
    /**
     * For adapters that implement it, this method notifies the analytics service that an anonymous user now has a unique identifier
     * @param {...Array<unknown>} args optional arguments to pass to the adapter handler
     */
    alias(...args: Array<unknown>): void;

    /**
     * Tracks when a user, i.e. a visitor who has a unique identity with the app is active
     * For analytics services that have identification functionality
     * @param {...Array<unknown>} args optional arguments to pass to the adapter handler
     */
    identify(...args: Array<unknown>): void;

    /**
     *
     * Tracks when an event such as a custom event is triggered within the application
     * @param {...Array<unknown>} args optional arguments to pass to the adapter handler
     */
    trackEvent(...args: Array<unknown>): void;

    /**
     * Used by analytics services to track page views
     * Due to the way Single Page Applications implement routing, you will need to call this on the activate hook of each route to track all page views
     * @param {...Array<unknown>} args optional arguments to pass to the adapter handler
     */
    trackPage(...args: Array<unknown>): void;

    /**
     * Instantiates the adapters specified in the configuration and caches them
     * for future retrieval.
     * @param {Array<IAdapterOptions>} adapterOptions adapter configuration options
     * @returns {Array<IAdapterOptions>} the list of now instantiated adapters
     */
    activateAdapters<T extends BaseAdapter>(adapterOptions: Array<IAdapterOptions>): Record<string, T>;

    /**
     * Invokes a method on the passed adapter, or across all activated adapters if not passed
     * @param {string} methodName name of the adapter method to be invoked
     * @param {...Array<unknown>} [args] additional optional args to be sent to the invoked method
     */
    invoke(methodName: string, ...args: Array<unknown>): void;
  }
}

declare module 'ember-metrics/metrics-adapters/base' {
  import EmberObject from '@ember/object';

  /**
   * Adapter interface to implemented by extending adapter implementations
   * @export
   * @abstract
   * @class BaseAdapter
   * @extends {EmberObject}
   */
  export default abstract class BaseAdapter extends EmberObject {
    /**
     * init method to be implemented by the extending adapter
     */
    init(): void;

    /**
     * willDestroy life-cycle hook to be implemented by the extending adapter
     */
    willDestroy(): void;

    /**
     * Serializes the adapter to a string with a guid and name if provided by a `toStringExtension` function
     * formatted as: `ember-metrics@metrics-adapter:${extension}:${guid}`
     */
    toString(): string;

    /**
     *
     * Tracks when an event such as a custom event is triggered within the application
     * @param {...Array<unknown>} args optional arguments to pass to the adapter handler
     */
    trackEvent(...args: Array<unknown>): void;

    /**
     * Used by analytics services to track page views
     * Due to the way Single Page Applications implement routing, you will need to call this on the activate hook of each route to track all page views
     * @param {...Array<unknown>} args optional arguments to pass to the adapter handler     *
     */
    trackPage(...args: Array<unknown>): void;

    /**
     * Tracks when a user, i.e. a visitor who has a unique identity with the app is active
     * For analytics services that have identification functionality
     * @param {...Array<unknown>} args optional arguments to pass to the adapter handler     *
     */
    identify(...args: Array<unknown>): void;

    /**
     * For adapters that implement it, this method notifies the analytics service that an anonymous user now has a unique identifier
     * @param {...Array<unknown>} args optional arguments to pass to the adapter handler     *
     */
    alias(...args: Array<unknown>): void;
  }
}
