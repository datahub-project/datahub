/**
 * This module is typings for the dynamic-link ember addon. The documentation for this component
 * and interface can be found at: https://github.com/asross/dynamic-link
 */
declare module 'dynamic-link/components/dynamic-link' {
  import Component from '@ember/component';

  interface IDynamicLinkParams {
    rel?: string;
    title?: string;
    text?: string;
    target?: string;
    tabIndex?: string;
    className?: string;
    href?: string;
    route?: string;
    model?: string | Array<unknown> | Record<string, unknown>;
    action?: string;
    queryParams?: Record<string, unknown>;
    activeClass?: string;
    activeWhen?: string;
    bubbles?: boolean;
  }

  export default class DynamicLink extends Component {
    attributeBindings: Array<string>;
    params: IDynamicLinkParams;
    defaultActiveClass?: string;
    rel: IDynamicLinkParams['rel'];
    title: IDynamicLinkParams['title'];
    target: IDynamicLinkParams['target'];
    tabIndex: IDynamicLinkParams['tabIndex'];
    className: IDynamicLinkParams['className'];
    route: IDynamicLinkParams['route'];
    model: IDynamicLinkParams['model'];
    action: IDynamicLinkParams['action'];
    queryParams: IDynamicLinkParams['queryParams'];
    activeClass: IDynamicLinkParams['activeClass'];
    activeWhen: IDynamicLinkParams['activeWhen'];
    activeClassName: string;
    bubbles: IDynamicLinkParams['bubbles'];
    models: Array<unknown>;
    routeArguments: [IDynamicLinkParams['route'], ...DynamicLink['models']];
    routingArguments: [IDynamicLinkParams['route'], DynamicLink['models'], IDynamicLinkParams['queryParams']];
    href: string;
    click(event: MouseEvent): boolean;
    performAction(): void;
    transitionRoute(): void;
    isActive: boolean;
  }
}
