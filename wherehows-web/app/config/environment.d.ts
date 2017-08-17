export default config;

/**
 * Type declarations for
 *    import config from './config/environment'
 *
 * For now these need to be managed by the developer
 * since different ember addons can materialize new entries.
 */
declare namespace config {
  export const environment: any;
  export const modulePrefix: string;
  export const podModulePrefix: string;
  export const locationType: string;
}
