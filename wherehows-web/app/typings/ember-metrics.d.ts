declare module 'ember-metrics' {
  import Service from '@ember/service';

  export default class Metrics extends Service {
    alias(...args: Array<any>): void;
    identify(...args: Array<any>): void;
    trackEvent(...args: Array<any>): void;
    trackPage(...args: Array<any>): void;
    activateAdapters<T = {}>(options: Array<T>): Array<T>;
    invoke(methodName: string, ...args: Array<any>): void;
  }
}
