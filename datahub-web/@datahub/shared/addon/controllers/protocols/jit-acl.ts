import { action, set } from '@ember/object';
import { IProtocolDefinition } from '@datahub/utils/types/controllers';
import { ProtocolController } from '@datahub/utils/controllers/protocol';

/**
 * JIT ACL Protocol will shared common functionality for request_jit_urns
 */
export const JitAclProtocol: IProtocolDefinition = {
  protocol: (ControllerClass: typeof ProtocolController): typeof ProtocolController => {
    class Protocol extends ControllerClass {
      /**
       * A list of dataset urns to request for JIT ACL Access provided as a query param
       * The query param renders the request-access modal if there are urns provided
       * @type {Array<string>}
       */
      request_jit_urns: Array<string> = [];

      /**
       * Action passed as UI control for adding a dataset urn to the 'urns' query param
       * @param urn dataset urn added
       */
      @action
      addRequestJitUrn(urn: string): void {
        const requestJitUrns = this.request_jit_urns;

        if (!requestJitUrns.includes(urn)) {
          set(this, 'request_jit_urns', [...requestJitUrns, urn]);
        }
      }

      /**
       * Action passed as UI control for removing a dataset urn from the 'urns' query param
       * @param urn dataset urn filtered out
       */
      @action
      removeRequestJitUrn(urn: string): void {
        const requestJitUrns = this.request_jit_urns;
        set(
          this,
          'request_jit_urns',
          [...requestJitUrns].filter((requestJitUrn: string): boolean => requestJitUrn !== urn)
        );
      }

      /**
       * Action passed as UI control for resetting the request_jit_urns array to an empty array
       */
      @action
      resetRequestJitUrns(): void {
        set(this, 'request_jit_urns', []);
      }
    }
    return Protocol;
  },
  queryParams: ['request_jit_urns']
};
