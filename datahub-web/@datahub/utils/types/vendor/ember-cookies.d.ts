declare module 'ember-cookies' {
  import Service from '@ember/service';
  export type EmberCookieService = Service & {
    write(name: string, value: string): void;
    read(name: string): string | undefined;
  };
}
