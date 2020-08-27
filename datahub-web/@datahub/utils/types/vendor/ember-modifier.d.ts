// Adds typescripting declarations for ember-modifier since the addon was apparently written in .js
// without .ts support
declare module 'ember-modifier' {
  /**
   * Functional modifiers consist of a function that receives:
   * - The element
   * - An array of positional arguments
   * - An object of named arguments
   *
   * This function runs the first time when the element the modifier was applied to is inserted
   * into the DOM, and it autotracks while running.Any values that it accesses will be tracked,
   * including the arguments it receives, and if any of them changes, the function will run again.
   *
   * The modifier can also optionally return a destructor.The destructor function will be run just
   * before the next update, and when the element is being removed entirely.It should generally
   * clean up the changes that the modifier made in the first place.
   * @param element - the element the modifier is attached to
   * @param params - positional arguments, similar to an Ember helper
   * @param hash - named arguments, similar to an Ember helper
   */
  // Note: Since these modifiers can be used in a template and allow the consumer to specify any
  // type of paramter, ignoring the eslint issue to accommodate. Record<string, unknown> does not
  // work well when we're trying to specify different, but valid interfaces for params/hash
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  function modifierFunc(element: HTMLElement, params?: Array<any>, hash?: Record<string, any>): Function;
  export function modifier(callback: typeof modifierFunc): void;

  /**
   * Sometimes you may need to do something more complicated than what can be handled by functional
   * modifiers. For instance:
   * - You may need to inject services and access them
   * - You may need fine-grained control of updates, either for performance or convenience reasons,
   *   and don't want to teardown the state of your modifier every time only to set it up again.
   * - You may need to store some local state within your modifier.
   *
   * In these cases, you can use a class modifier instead.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  class Modifier<T extends { positional?: Array<any>; named?: Record<string, any> }> {
    protected args: T;
    protected element: HTMLElement;
    /**
     * Lifecycle event hook run when the helper receives args
     */
    didReceiveArguments(): void;
    /**
     * Run when we are removing the element this modifier is attached to
     */
    willRemove(): void;
    /**
     * Lifecycle event hook run when the element has been inserted into the DOM
     */
    didInstall(): void;
    constructor(...args: Array<unknown>);
  }
  export default Modifier;
}
