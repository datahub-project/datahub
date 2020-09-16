declare module 'ember-load-initializers' {
  import Application from '@ember/application';

  export default function(app: typeof Application, prefix: string): void;
}
