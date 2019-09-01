import Service from '@ember/service';

export default class MetricsServiceStub extends Service {
  alias() {}
  identify() {}
  trackEvent() {}
  trackPage() {}
  activateAdapters<T>(adapters: Array<T>) {
    return adapters;
  }
  invoke(_methodName: string) {}
}
