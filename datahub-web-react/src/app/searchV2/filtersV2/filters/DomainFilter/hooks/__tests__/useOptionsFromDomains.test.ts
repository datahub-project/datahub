import { Domain, EntityType } from '@src/types.generated';
import { renderHook } from '@testing-library/react-hooks';
import useOptionsFromDomains from '../useOptionsFromDomains';

function getSampleDomain(urn: string, parents: Domain[] = []): Domain {
    return {
        urn,
        id: urn,
        type: EntityType.Domain,
        properties: { name: urn },
        parentDomains: {
            count: parents.length,
            domains: parents,
        },
    };
}

function renderLabel(domain: Domain) {
    return domain.urn;
}

describe('useOptionsFromDomains', () => {
    it('should handle nested domains when parents presetned explicitly', () => {
        const parentDomain = getSampleDomain('parent');
        const childDomain = getSampleDomain('child', [parentDomain]);
        const nestedChildDomain = getSampleDomain('nestedChild', [childDomain, parentDomain]);

        const response = renderHook(() =>
            useOptionsFromDomains([parentDomain, childDomain, nestedChildDomain], renderLabel),
        ).result.current;

        expect(response).toStrictEqual([
            {
                value: parentDomain.urn,
                label: parentDomain.urn,
                entity: parentDomain,
                isParent: true,
                parentValue: undefined,
            },
            {
                value: childDomain.urn,
                label: childDomain.urn,
                entity: childDomain,
                isParent: true,
                parentValue: parentDomain.urn,
            },
            {
                value: nestedChildDomain.urn,
                label: nestedChildDomain.urn,
                entity: nestedChildDomain,
                isParent: false,
                parentValue: childDomain.urn,
            },
        ]);
    });

    it('should handle nested domains when parents presented implicitly', () => {
        const parent = getSampleDomain('parent');
        const child = getSampleDomain('child');
        const nested = getSampleDomain('nested', [child, parent]);

        const response = renderHook(() => useOptionsFromDomains([nested], renderLabel)).result.current;

        expect(response).toStrictEqual([
            {
                value: nested.urn,
                label: nested.urn,
                entity: nested,
                isParent: false,
                parentValue: child.urn,
            },
            {
                value: child.urn,
                label: child.urn,
                entity: { ...child, parentDomains: { count: 1, domains: [parent] } },
                isParent: true,
                parentValue: parent.urn,
            },
            {
                value: parent.urn,
                label: parent.urn,
                entity: parent,
                isParent: true,
                parentValue: undefined,
            },
        ]);
    });

    it('should handle different domains', () => {
        const domain = getSampleDomain('domain');
        const another = getSampleDomain('another');

        const response = renderHook(() => useOptionsFromDomains([domain, another], renderLabel)).result.current;

        expect(response).toStrictEqual([
            {
                value: domain.urn,
                label: domain.urn,
                entity: domain,
                isParent: false,
                parentValue: undefined,
            },
            {
                value: another.urn,
                label: another.urn,
                entity: { ...another, parentDomains: { count: 0, domains: [] } },
                isParent: false,
                parentValue: undefined,
            },
        ]);
    });

    it('should handle duplicates', () => {
        const domain = getSampleDomain('domain');
        const duplicate = getSampleDomain('domain');

        const response = renderHook(() => useOptionsFromDomains([domain], renderLabel)).result.current;

        expect(response).toStrictEqual([
            {
                value: duplicate.urn,
                label: duplicate.urn,
                entity: duplicate,
                isParent: false,
                parentValue: undefined,
            },
        ]);
    });
});
