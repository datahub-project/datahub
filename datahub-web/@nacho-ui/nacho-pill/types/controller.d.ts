import Demo from 'dummy/tests/dummy/app/controllers/demo';

declare module '@ember/controller' {
  // eslint-disable-next-line @typescript-eslint/interface-name-prefix
  interface Registry {
    demo: Demo;
  }
}
