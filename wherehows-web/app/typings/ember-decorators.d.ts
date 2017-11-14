declare module 'ember-decorators' {
  export namespace Decorators {
    const action: MethodDecorator;
  }
  export default Decorators;
}

declare module 'ember-decorators/object' {
  import Decorators from 'ember-decorators';
  export const action: typeof Decorators.action;
}

declare module 'ember-decorators/component';

declare module 'ember-decorators/controller';

declare module 'ember-decorators/data';

declare module 'ember-decorators/service';

declare module 'ember-decorators/utils';
