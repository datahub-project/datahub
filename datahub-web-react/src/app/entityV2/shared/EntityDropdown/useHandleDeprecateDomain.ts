import { useDomainsContext } from '@app/domainV2/DomainsContext';

import { Entity, EntityType } from '@types';

export type DeprecationFormData = {
    note?: string | null;
    decommissionTime?: number | null;
    replacement?: Entity | null;
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
                      replacement: formData?.replacement ?? null,
                  }
                : null,
        });
    };

    return { handleDeprecateDomainComplete };
}
