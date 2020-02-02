/**
 * Generalized dynamic component to render for {{component}} helper
 *
 * @interface IDynamicComponent
 */
export interface IDynamicComponent {
  // name for the component to render
  name: string;
  // additional options that can be passed into the component
  options?: Record<string, unknown>;
}
