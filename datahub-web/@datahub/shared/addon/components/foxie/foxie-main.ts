import Component from '@glimmer/component';
import { inject as service } from '@ember/service';
import { computed, action } from '@ember/object';
import { alias } from '@ember/object/computed';
import FoxieService from '@datahub/shared/services/foxie';
import { IConfigurator } from '@datahub/shared/types/configurator/configurator';
import { tracked } from '@glimmer/tracking';

/**
 * Interface for the mouse co-ordinates for the foxie main element
 */
type IPositionGroup = {
  a: number;
  b: number;
  c: number;
  d: number;
};

export default class FoxieMain extends Component<{}> {
  /**
   * Injection of the Foxie service as our main way of updating state for this component
   */
  @service
  foxie!: FoxieService;

  @service
  configurator!: IConfigurator;

  /**
   * Toggled by the user if they want to expand the content currently available in foxie
   */
  @tracked
  isExpanded = false;

  /**
   * Local variable incharge of holding the coordinates
   */
  positionCoordinates: IPositionGroup = {
    a: 0,
    b: 0,
    c: 0,
    d: 0
  };

  /**
   * Alias to determine what the current rendering configurations should be
   */
  @alias('foxie.currentTriggeredAction')
  currentTriggeredAction!: FoxieService['currentTriggeredAction'];

  @alias('foxie.isActive')
  isFoxieActive!: boolean;

  /**
   * Ensures we only show this component when the service is not flag guarded
   */
  @computed('configurator')
  get showFoxieFlag(): boolean {
    return Boolean(this.configurator.getConfig('showFoxie', { useDefault: true, default: false }));
  }

  @computed('isExpanded', 'currentTriggeredAction')
  get currentImageState(): string {
    const { isExpanded, currentTriggeredAction } = this;

    const hasTriggeredAction = Boolean(currentTriggeredAction);
    return isExpanded ? 'talking' : hasTriggeredAction ? 'active' : 'asleep';
  }

  @computed('currentImageState')
  get imageSource(): string {
    return `foxie/sprites/foxie-${this.currentImageState}`;
  }

  @action
  initEventListener(element: HTMLElement): void {
    element.addEventListener('mousedown', this.dragMouseDown);
    element.addEventListener('dblclick', this.toggleExpanded);
  }

  /**
   * Function that dictates the drag logic for the FoxieMainElement
   * @param e mouse event
   */
  elementDrag(e: MouseEvent): void {
    e = e || window.event;
    e.preventDefault();
    // calculate the new cursor position:
    this.positionCoordinates.a = this.positionCoordinates.c - e.clientX;
    this.positionCoordinates.b = this.positionCoordinates.d - e.clientY;
    this.positionCoordinates.c = e.clientX;
    this.positionCoordinates.d = e.clientY;
    // set the element's new position:
    const foxieMainElement: HTMLElement = document.querySelector('.foxie') as HTMLElement;

    foxieMainElement.style.top = foxieMainElement.offsetTop - this.positionCoordinates.b + 'px';
    foxieMainElement.style.left = foxieMainElement.offsetLeft - this.positionCoordinates.a + 'px';
  }

  /**
   * Function that resets the moving after user releases the element on dragging.
   */
  closeDragElement(): void {
    // stop moving when mouse button is released:
    document.onmouseup = null;
    document.onmousemove = null;
  }

  @action
  toggleExpanded(): void {
    this.isExpanded = !this.isExpanded;
    if (!this.isExpanded) {
      this.foxie.onDismiss();
    }
  }

  @action
  onDismiss(): void {
    this.isExpanded = false;
    this.foxie.onDismiss();
  }

  /**
   * Handles the mouseDown event and saves the X and Y co-ordinates when user starts dragging and passes on the dragging and closing logic over
   * from the default document handlers to overrides.
   * @param e MouseEvent
   */
  @action
  dragMouseDown(e: MouseEvent): void {
    e = e || window.event;
    e.preventDefault();

    this.positionCoordinates.c = e.clientX;
    this.positionCoordinates.d = e.clientY;

    document.onmouseup = this.closeDragElement.bind(this);
    document.onmousemove = this.elementDrag.bind(this);
  }
}
