import { useDomainsContext } from '@app/domainV2/DomainsContext';

import { EntityType } from '@types';

export type DeprecationFormData = {
    note?: string | null;
    decommissionTime?: number | null;
};

export function useHandleDeprecateDomain(urn: string) {
    const { setUpdatedDomain } = useDomainsContext();

    const handleDeprecateDomainComplete = (deprecated: boolean, formData?: DeprecationFormData) => {
        setUpdatedDomain({
            urn,
            type: EntityType.Domain,
            id: urn.split(':').pop() ?? urn,
            deprecation: deprecated
                ? {
                      deprecated: true,
                      note: formData?.note ?? null,
                      actor: null,
                      decommissionTime: formData?.decommissionTime ?? null,
                  }
                : null,
        });
    };

    return { handleDeprecateDomainComplete };
}
