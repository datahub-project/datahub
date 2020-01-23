import Component from '@ember/component';
import { TestContext } from 'ember-test-helpers';
import { render } from '@ember/test-helpers';
import { dasherize } from '@ember/string';
import { TemplateFactory } from 'htmlbars-inline-precompile';

/**
 * Defines the argument shape to be passed to getRenderedComponent
 * @interface IArgs
 * @template T the testing environment execution context
 * @template C component to be rendered which should extend the Ember.Component class
 */
interface IArgs<T, C> {
  // Component class to be registered, rendered and instantiated
  ComponentToRender: new (...args: Array<unknown>) => C;
  // Execution context for the running test
  testContext: T;
  // hbs template string for the component to be instantiated
  template: TemplateFactory;
  // alternate name for the component being rendered, this has utility when the component definition is in a nested
  // directory and ComponentToRender.name is not a valid reference to the component. e.g. top-folder/my-nested-component
  componentName?: string;
}

/**
 * Testing helper to render an Ember component from a supplied hbs template and resolve with a reference to the
 * instantiated component post render
 */
export const getRenderedComponent = async <C extends Component, T extends TestContext>({
  ComponentToRender,
  testContext,
  template,
  componentName
}: IArgs<T, C>): Promise<C> => {
  let component: C | undefined;
  componentName = componentName || dasherize(ComponentToRender.name).toLowerCase();

  testContext.owner.register(
    `component:${componentName}`,
    // TS expects that the Base constructor return type C be statically known to extend safely. Since C is constrained to type Ember.Component
    // @ts-ignore (2): and we are only using Component#didInsertElement element on the constraining class, this is a type safe extension
    class extends ComponentToRender {
      didInsertElement(this: C): void {
        super.didInsertElement();
        component = this;
      }
    }
  );

  await render(template);

  if (!component) {
    throw new Error(`Could not obtain reference to rendered component ${ComponentToRender.name}`);
  }

  return component;
};
