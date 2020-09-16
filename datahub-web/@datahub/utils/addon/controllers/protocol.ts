import Controller from '@ember/controller';
import { IProtocolDefinition } from '@datahub/utils/types/controllers';

/**
 * Default Protocol controller which other protocols must extend
 */
export class ProtocolController extends Controller {
  /**
   * This method will be called to clear attributes when exiting the route on which
   * the controller is used.
   *
   * Make sure you call super.resetProtocol when overriding to avoid breaking the chain.
   */
  resetProtocol(): void {
    //noop
  }
}

/**
 * Mixin kind of way to share common logic accross Controllers (by @sadebajo)
 * Usage:
 *
 * const myProtocol1 = {
 *   protocol: class extends ProtocolController {
 *      ...add your logic here
 *   },
 *   queryParams: ['your_query_params']
 * }
 *
 *
 * class MyController extends WithControllerProtocol(myProtocol1, myProtocol2, ...) {
 *
 *
 * }
 *
 * @param protocols Array of protocol that current controller will extend
 */
export const WithControllerProtocol = (...protocols: Array<IProtocolDefinition>): typeof ProtocolController =>
  protocols.reduce(
    (controller, { protocol, queryParams }): typeof ProtocolController =>
      class extends protocol(controller) {
        constructor() {
          // eslint-disable-next-line prefer-rest-params
          super(...arguments);
          const resolvedQueryParams = [].concat.apply(queryParams);
          this.queryParams = [...this.queryParams, ...resolvedQueryParams];
        }
      },
    ProtocolController
  );
