declare module 'ember-modal-dialog/components/modal-dialog';

declare module 'ember-simple-auth/mixins/authenticated-route-mixin' {
  import Mixin from '@ember/object/mixin';
  export default Mixin;
}

declare module 'ember-simple-auth/authenticators/base' {
  import EmberObject from '@ember/object';
  export default EmberObject;
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

declare module 'wherehows-web/utils/datasets/compliance-policy';
declare module 'wherehows-web/controllers/datasets/dataset' {
  import Controller from '@ember/controller';
  import { Tabs } from 'wherehows-web/constants/datasets/shared';

  export default class extends Controller {
    tabSelected: Tabs;
  }
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
}

/**
 * Merges the JSONView plugin into the jquery interface
 */
interface JQuery {
  JSONView(json: object): this;
  treegrid(): this;
}
