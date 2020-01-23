declare module 'ember-metrics' {
  import Service from '@ember/service';

  export default class Metrics extends Service {
    alias(...args: Array<unknown>): void;
    identify(...args: Array<unknown>): void;
    trackEvent(...args: Array<unknown>): void;
    trackPage(...args: Array<unknown>): void;
    activateAdapters<T = {}>(options: Array<T>): Array<T>;
    invoke(methodName: string, ...args: Array<unknown>): void;
  }
}
