import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../../templates/components/nacho-animation/pendulum-ellipsis';
import { classNames } from '@ember-decorators/component';

@classNames('nacho-ellipsis-animation')
export default class NachoAnimationPendulumEllipsis extends Component {
  layout = layout;
}
