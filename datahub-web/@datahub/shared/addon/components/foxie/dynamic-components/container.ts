import Component from '@glimmer/component';
import { IDynamicComponent } from '@datahub/shared/types/dynamic-component';

interface IFoxieDynamicComponentsContainerArgs {
  options: {
    components: Array<IDynamicComponent>;
  };
}

export default class FoxieDynamicComponentsContainer extends Component<IFoxieDynamicComponentsContainerArgs> {}
