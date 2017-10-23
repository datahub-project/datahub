declare module 'ember-modal-dialog/components/modal-dialog';

declare module 'ember-simple-auth/mixins/authenticated-route-mixin' {
  import Mixin from '@ember/object/mixin';
  export default Mixin.create({});
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

declare module 'ember-cli-mirage';

declare module 'ember-concurrency' {
  class TaskInstance {}
  class TaskProperty {
    perform(...args: Array<any>): TaskInstance;
    on(): this;
    cancelOn(eventNames: string): this;
    debug(): this;
    drop(): this;
    restartable(): this;
    enqueue(): this;
    keepLatest(): this;
    performs(): this;
    maxConcurrency(n: number): this;
  }
  export function task(...args: Array<any>): TaskProperty;
  export function timeout(delay: number): Promise<void>;
}

// https://github.com/ember-cli/ember-fetch/issues/72
// TS assumes the mapping btw ES modules and CJS modules is 1:1
// However, `ember-fetch` is the module name, but it's imported with `fetch`
declare module 'fetch' {
  export default function fetch(input: RequestInfo, init?: RequestInit): Promise<Response>;
}

/**
 * Merges global type defs for global modules on the window namespace.
 * These should be refactored into imported modules if available or shimmed as such
 */
interface Window {
  marked(param: string): { htmlSafe: () => string };
}
