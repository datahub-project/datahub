declare module 'ember-simple-auth/mixins/authenticated-route-mixin' {
  import MixinOne from '@ember/object/mixin';
  export default MixinOne;
}

declare module 'ember-simple-auth/mixins/application-route-mixin' {
  import MixinTwo from '@ember/object/mixin';
  export default MixinTwo;
}

declare module 'ember-simple-auth/services/session' {
  import Ember from 'ember';
  import Service from '@ember/service';
  import Transition from '@ember/routing/-private/transition';

  export default class Session extends Service {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    authenticate(authenticator: string, ...args: Array<any>): Promise<void>;
    authorize(authorizer: string, block: Function): void;
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    invalidate(...args: Array<any>): Promise<void>;
    attemptedTransition: Transition | null;
    data: Readonly<object | { authenticated: {} }>;
    isAuthenticated: boolean;
  }
}

declare module 'ember-simple-auth/authenticators/base' {
  import EmberObject from '@ember/object';
  export default EmberObject;
}
