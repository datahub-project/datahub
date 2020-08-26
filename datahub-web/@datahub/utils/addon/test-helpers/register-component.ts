import EmberComponent from '@ember/component';
import GlimmerComponent from '@glimmer/component';
import { TestContext } from 'ember-test-helpers';
import { render } from '@ember/test-helpers';
import { dasherize } from '@ember/string';
import { TemplateFactory } from 'htmlbars-inline-precompile';
import { IConstructor } from '@datahub/utils/types/base';

/**
 * Defines the argument shape to be passed to getRenderedComponent
 * @interface IArgs
 * @template T the testing environment execution context
 * @template C component to be rendered which should extend the Ember.Component class
 */
interface IArgs<T, C> {
  // Component class to be registered, rendered and instantiated
  ComponentToRender: IConstructor<C>;
  // Execution context for the running test
  testContext: T;
  // hbs template string for the component to be instantiated
  template: TemplateFactory;
  // alternate name for the component being rendered, this has utility when the component definition is in a nested
  // directory and ComponentToRender.name is not a valid reference to the component. e.g. top-folder/my-nested-component
  componentName?: string;
}

/**
 * Discriminating type union guard between an Ember or Glimmer Component
 * @param {(IConstructor<EmberComponent | GlimmerComponent>)} component A Glimmer or Ember component class
 */
const isEmberComponent = (
  component: IConstructor<EmberComponent | GlimmerComponent>
): component is IConstructor<EmberComponent> => component.prototype.hasOwnProperty('didInsertElement');

/**
 * Testing helper to render an Ember component from a supplied hbs template and resolve with a reference to the
 * instantiated component post render
 */
export const getRenderedComponent = async <C extends EmberComponent | GlimmerComponent, T extends TestContext>({
  ComponentToRender,
  testContext,
  template,
  componentName
}: IArgs<T, C>): Promise<C> => {
  let component: C | undefined;
  componentName = componentName || dasherize(ComponentToRender.name).toLowerCase();

  const ComponentTestDouble = isEmberComponent(ComponentToRender)
    ? class extends (ComponentToRender as IConstructor<EmberComponent>) {
        didInsertElement(this: C): void {
          super.didInsertElement();
          component = this;
        }
      }
    : class extends (ComponentToRender as IConstructor<GlimmerComponent>) {
        constructor(...args: Array<unknown>) {
          super(...args);
          component = (this as unknown) as C;
        }
      };

  testContext.owner.register(`component:${componentName}`, ComponentTestDouble);

  await render(template);

  if (!component) {
    throw new Error(`Could not obtain reference to rendered component ${ComponentToRender.name}`);
  }

  return component;
};
