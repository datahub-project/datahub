import Component from '@ember/component';
import { setProperties } from '@ember/object';
import { run } from '@ember/runloop';
// @ts-ignore: Ignore import of compiled template
import template from '../templates/components/big-list';
import { layout, tagName } from '@ember-decorators/component';
import { noop } from 'lodash';

type OnFinishedReturnType<T> = void | Promise<Array<T>>;
type OnFinishedType = <T>(currentIndex: number) => OnFinishedReturnType<T>;

/**
 * Big list is a component that will render a list of items using request animation frames.
 *
 * The goal is to achieve less than 16 ms per frame to render. Since the user will specify
 * the block template that will be used for every item, depending on how complex it is that template,
 * the use can play with chunkSize to make it load faster if the template is light.
 *
 * When the list has finished loading, there is an action will be trigger with the last index rendered. At that point
 * the user has the option to not return anything, or to return a promise that will resolve in more items to append to the list.
 * That behavior is handy to load more server side items when the ui list has finished rendering.
 *
 * Example:
 *
 * <BigList @list={{items}} @chunkSize={{chunkSize}} @onFinished={{onFinished}} as |item|>
 *   <span class="number">{{item}}</span>
 * </BigList>
 */
@tagName('')
@layout(template)
export default class BigList<T> extends Component {
  // input list to render
  list?: Array<T> = [];
  // internal list to be used in the template as the actual list that is rendered
  renderedList: Array<T> = [];
  // the number of items that are going to be copied from list to renderedList per loop
  chunkSize = 1;
  // the last index of list that was copied into renderedList
  currentIndex = 0;
  // an ID to refer to the animation frame
  animationFrameId?: number;
  // a closure action that will be passed in when the parent component wants to know when the list has been rendered
  onFinished: OnFinishedType = noop;

  /**
   * Hook to start render items when the list is in dom
   */
  didInsertElement(): void {
    super.didInsertElement();
    this.process();
  }

  /**
   * Will request an animation frame to make sure the last one is render, and invoke
   * the next item to render.
   * We will save an ID for the animation frame just in cases our component gets destroyed before it is executed
   */
  process(): void {
    const animationFrameId = window.requestAnimationFrame(() => run(() => this.next()));
    setProperties(this, {
      animationFrameId
    });
  }

  /**
   * Will render the next item by coping some items (chunkSize) from list to renderedList.
   *
   * The current index will advaced either to the next index (batch + index) or the end of the array.
   *
   * Once the end of the array is reached, it will invoke onFinished action.
   */
  next(): void {
    const { chunkSize, list, currentIndex, renderedList } = this;

    if (list && list.length > currentIndex) {
      const nextIndex = Math.min(currentIndex + chunkSize, list.length);
      renderedList.addObjects(list.slice(currentIndex, nextIndex));
      setProperties(this, {
        currentIndex: nextIndex
      });
      this.process();
      return;
    }

    this.handleOnFinishedResponse(this.onFinished(currentIndex));
  }

  /**
   * Will handle the response of onFinished.
   * If empty nothing to be done
   * If promise, then await promise and then add the result to the list and
   * resume the process again.
   * @param onFinishedReturn the return of the onFinished fn
   */
  async handleOnFinishedResponse(onFinishedReturn: OnFinishedReturnType<T>): Promise<void> {
    if (onFinishedReturn) {
      const additionalList = await onFinishedReturn;
      const { list } = this;
      if (additionalList.length > 0 && list) {
        list.addObjects(additionalList);
        this.process();
      }
    }
  }

  /**
   * If the element is going to be destroyed, then cancel the current animation frame
   */
  willDestroyElement(): void {
    const { animationFrameId } = this;
    if (animationFrameId) {
      window.cancelAnimationFrame(animationFrameId);
    }
  }
}
