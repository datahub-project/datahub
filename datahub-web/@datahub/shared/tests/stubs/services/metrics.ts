import Service from '@ember/service';

export default class MetricsServiceStub extends Service {
  alias(): void {}
  identify(): void {}
  trackEvent(): void {}
  trackPage(): void {}
  activateAdapters<T>(adapters: Array<T>): Array<T> {
    return adapters;
  }
  invoke(_methodName: string): void {}
}
