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

/**
 * A slightly stronger typed version of IDynamicComponent that helps us type the options property
 * to a specific component's expected args
 */
export interface IDynamicComponentArgs<T extends { options: Record<string, unknown> }> {
  // name for the component to render
  name: string;
  // additional options that can be passed into the component
  options: T['options'];
}
