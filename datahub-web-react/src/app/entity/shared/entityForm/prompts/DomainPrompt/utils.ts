import { FormPrompt } from '@src/types.generated';
import { GenericEntityProperties } from '../../../types';

// get initial domain to show in an unfinished form prompt
export function getDefaultDomain(entityData: GenericEntityProperties | null, prompt: FormPrompt) {
    const currentDomain = entityData?.domain?.domain;
    if (!currentDomain) {
        return null;
    }

    const allowedDomainUrns = prompt.domainParams?.allowedDomains?.map((d) => d.urn);
    if (allowedDomainUrns?.length) {
        return allowedDomainUrns.includes(currentDomain.urn) ? currentDomain : null;
    }

    return currentDomain;
}
