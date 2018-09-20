declare module '@ember-decorators/runloop';

declare module 'ember-modal-dialog/components/modal-dialog';

declare module 'ember-radio-button/components/radio-button';

declare module 'ember-simple-auth/mixins/authenticated-route-mixin' {
  import Mixin from '@ember/object/mixin';
  export default Mixin;
}

declare module 'wherehows-web/initializers/ember-cli-mirage' {
  import { IMirageServer } from 'wherehows-web/typings/ember-cli-mirage';

  const startMirage: (env?: string) => IMirageServer;

  export { startMirage };
}

declare module 'ivy-tabs/components/ivy-tabs-tablist' {
  import Component from '@ember/component';
  export default class IvyTabsTablistComponent extends Component {
    focusSelectedTab: () => void;
    selectPreviousTab: () => void;
    selectNextTab: () => void;
  }
}

declare module 'ember-simple-auth/authenticators/base' {
  import EmberObject from '@ember/object';
  export default EmberObject;
}

/**
 * Augments the Transition interface in the Ember namespace
 */
declare module 'ember' {
  export namespace Ember {
    // eslint-disable-next-line typescript/interface-name-prefix
    interface Transition {
      params: Record<string, any>;
      // temporary typedef till resolution of https://github.com/tildeio/router.js/issues/250
      promise?: Promise<any> & { finally: (callback: Function, label?: string) => Promise<any> };
    }
  }
}

declare module 'ember-simple-auth/services/session' {
  import Ember from 'ember';
  import Service from '@ember/service';

  export default class Session extends Service {
    authenticate(authenticator: string, ...args: Array<any>): Promise<void>;
    authorize(authorizer: string, block: Function): void;
    invalidate(...args: Array<any>): Promise<void>;
    attemptedTransition: Ember.Transition | null;
    data: Readonly<object | { authenticated: {} }>;
    isAuthenticated: boolean;
  }
}

declare module 'ember-inflector' {
  const singularize: (arg: string) => string;
  function pluralize(arg: string): string;
  function pluralize(count: number, arg: string, options?: { withoutCount: number }): string;
  export { singularize, pluralize };
}

declare module 'ember-cli-mirage';

// https://github.com/ember-cli/ember-fetch/issues/72
// TS assumes the mapping btw ES modules and CJS modules is 1:1
// However, `ember-fetch` is the module name, but it's imported with `fetch`
declare module 'fetch' {
  export default function fetch(
    input: RequestInfo,
    options?: { method?: string; body?: any; headers?: object | Headers; credentials?: RequestCredentials }
  ): Promise<Response>;
}

declare module 'scrollmonitor';

/**
 * Merges global type defs for global modules on the window namespace.
 * These should be refactored into imported modules if available or shimmed as such
 */
// eslint-disable-next-line typescript/interface-name-prefix
interface Window {
  marked(param: string): { htmlSafe: () => string };

  JsonHuman: {
    format(
      data: any,
      options?: {
        showArrayIndex: void | boolean;
        hyperlinks: { enable: boolean; keys: null | Array<string>; target: string };
        bool: {
          //https://github.com/prettier/prettier/issues/3102
          text: object; //{ 'true': string; 'false': string };
          image: object; //{ 'true': string; 'false': string };
          showImage: boolean;
          showText: boolean;
        };
      }
    ): Element;
  };

  // global array for piwik tracking
  _paq: Array<any>;
}

/**
 * Merges the JSONView plugin into the jquery interface
 */
// eslint-disable-next-line typescript/interface-name-prefix
interface JQuery {
  JSONView(json: object): this;
  treegrid(): this;
}
