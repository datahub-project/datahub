import Component from '@glimmer/component';

interface IFoxieDynamicComponentsLinkButtonArgs {
  options: {
    text: string;
    linkTo: string;
    linkModel: unknown;
  };
}

export default class FoxieDynamicComponentsLinkButton extends Component<IFoxieDynamicComponentsLinkButtonArgs> {}
