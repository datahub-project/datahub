import { FormPrompt } from '@src/types.generated';
import { GenericEntityProperties } from '@src/app/entity/shared/types';
import { getDefaultDomain } from '../utils';

describe('getDefaultDomain', () => {
    const promptWithAllowedDomains = {
        id: '1',
        domainParams: { allowedDomains: [{ urn: 'urn:li:domain:test1' }, { urn: 'urn:li:domain:test2' }] },
    } as FormPrompt;
    const promptWithAnyDomains = {
        id: '1',
        domainParams: {},
    } as FormPrompt;
    const entityDataWithDomain = {
        domain: {
            domain: {
                urn: 'urn:li:domain:test1',
            },
        },
    } as GenericEntityProperties;
    const entityDataWithoutDomain = {
        domain: null,
    } as GenericEntityProperties;

    it('should return null if there is no domain on the asset yet', () => {
        const defaultDomain = getDefaultDomain(entityDataWithoutDomain, promptWithAnyDomains);
        expect(defaultDomain).toBe(null);
    });

    it('should return the current domain if there are no allowed domains', () => {
        const defaultDomain = getDefaultDomain(entityDataWithDomain, promptWithAnyDomains);
        expect(defaultDomain).toMatchObject({ urn: 'urn:li:domain:test1' });
    });

    it('should return the current domain if it is in the list of allowed domains', () => {
        const defaultDomain = getDefaultDomain(entityDataWithDomain, promptWithAllowedDomains);
        expect(defaultDomain).toMatchObject({ urn: 'urn:li:domain:test1' });
    });

    it('should return null if the current domain is not in the list of allowed domains', () => {
        const entityDataWithDomain2 = {
            domain: {
                domain: {
                    urn: 'urn:li:domain:notInAllowed',
                },
            },
        } as GenericEntityProperties;
        const defaultDomain = getDefaultDomain(entityDataWithDomain2, promptWithAllowedDomains);
        expect(defaultDomain).toBe(null);
    });
});
