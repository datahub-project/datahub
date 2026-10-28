import { GenericEntityProperties } from '@app/entity/shared/types';
import {
    canCreateTermVersion,
    canShowEditDeprecation,
    getDeprecationMenuActions,
    getSelectableStages,
    isGlossaryTermVersion,
    suggestNextVersionLabel,
} from '@app/entityV2/shared/EntityDropdown/utils';

import { EntityPrivileges, EntityType } from '@types';

describe('canShowEditDeprecation', () => {
    it('defaults to allowed when privileges are missing or the flag is unset', () => {
        expect(canShowEditDeprecation(undefined)).toBe(true);
        expect(canShowEditDeprecation(null)).toBe(true);
        expect(canShowEditDeprecation({} as EntityPrivileges)).toBe(true);
    });

    it('respects an explicit canEditDeprecation flag', () => {
        expect(canShowEditDeprecation({ canEditDeprecation: true } as EntityPrivileges)).toBe(true);
        expect(canShowEditDeprecation({ canEditDeprecation: false } as EntityPrivileges)).toBe(false);
    });
});

describe('getDeprecationMenuActions', () => {
    it('offers only "mark" when the entity is not deprecated', () => {
        expect(getDeprecationMenuActions(false, { canEditDeprecation: true } as EntityPrivileges)).toEqual([
            'markDeprecated',
        ]);
    });

    it('offers edit then un-deprecate when deprecated and editing is allowed', () => {
        expect(getDeprecationMenuActions(true, { canEditDeprecation: true } as EntityPrivileges)).toEqual([
            'editDeprecated',
            'markUnDeprecated',
        ]);
    });

    it('omits edit when the user cannot edit deprecation', () => {
        expect(getDeprecationMenuActions(true, { canEditDeprecation: false } as EntityPrivileges)).toEqual([
            'markUnDeprecated',
        ]);
    });
});

describe('getSelectableStages', () => {
    const DRAFT = 'urn:li:lifecycleStageType:DRAFT';
    const PUBLISHED = 'urn:li:lifecycleStageType:PUBLISHED';
    const ARCHIVED = 'urn:li:lifecycleStageType:ARCHIVED';
    const SYSTEM = 'urn:li:lifecycleStageType:SYSTEM';
    const stages = [
        { urn: DRAFT, name: 'Draft', userAssignable: true, allowedPreviousStages: null },
        { urn: PUBLISHED, name: 'Published', userAssignable: true, allowedPreviousStages: [DRAFT] },
        { urn: ARCHIVED, name: 'Archived', userAssignable: true, allowedPreviousStages: [] },
        { urn: SYSTEM, name: 'System', userAssignable: false, allowedPreviousStages: null },
    ];

    it('drops the current stage, unassignable stages and stages that do not accept the current one', () => {
        expect(getSelectableStages(stages, DRAFT).map((s) => s.urn)).toEqual([PUBLISHED]);
    });

    it('never offers a stage with an empty allowedPreviousStages list', () => {
        [DRAFT, PUBLISHED, null].forEach((current) =>
            expect(getSelectableStages(stages, current).map((s) => s.urn)).not.toContain(ARCHIVED),
        );
    });

    it('treats a term with no stage as the __NONE__ sentinel', () => {
        expect(getSelectableStages(stages, null).map((s) => s.urn)).toEqual([DRAFT]);
        expect(
            getSelectableStages(
                [{ ...stages[1], allowedPreviousStages: ['urn:li:lifecycleStageType:__NONE__'] }],
                undefined,
            ).map((s) => s.urn),
        ).toEqual([PUBLISHED]);
    });
});

describe('canCreateTermVersion', () => {
    const manager = { privileges: { canManageEntity: true } } as GenericEntityProperties;

    it('allows an unversioned term or the latest version for users who manage the term', () => {
        expect(canCreateTermVersion(true, manager)).toBe(true);
        expect(
            canCreateTermVersion(true, {
                ...manager,
                versionProperties: { isLatest: true },
            } as GenericEntityProperties),
        ).toBe(true);
    });

    it('hides the action on older versions, without the right, or with versioning off', () => {
        expect(
            canCreateTermVersion(true, {
                ...manager,
                versionProperties: { isLatest: false },
            } as GenericEntityProperties),
        ).toBe(false);
        expect(canCreateTermVersion(true, { privileges: { canManageEntity: false } } as GenericEntityProperties)).toBe(
            false,
        );
        expect(canCreateTermVersion(false, manager)).toBe(false);
        expect(canCreateTermVersion(true, null)).toBe(false);
    });
});

describe('suggestNextVersionLabel', () => {
    it('increments the trailing number and suggests v2 for an unversioned term', () => {
        expect(suggestNextVersionLabel('v9')).toBe('v10');
        expect(suggestNextVersionLabel('1')).toBe('2');
        expect(suggestNextVersionLabel('v1.0')).toBe('v1.1');
        expect(suggestNextVersionLabel('2024-Q1')).toBe('2024-Q2');
        expect(suggestNextVersionLabel(null)).toBe('v2');
    });

    it('suggests nothing without a trailing number', () => {
        expect(suggestNextVersionLabel('draft')).toBe('');
    });
});

describe('isGlossaryTermVersion', () => {
    const versioned = { versionProperties: { isLatest: true } } as GenericEntityProperties;

    it('is true only for a glossary term that belongs to a version set', () => {
        expect(isGlossaryTermVersion(EntityType.GlossaryTerm, versioned)).toBe(true);
        expect(isGlossaryTermVersion(EntityType.GlossaryTerm, {} as GenericEntityProperties)).toBe(false);
        expect(isGlossaryTermVersion(EntityType.Dataset, versioned)).toBe(false);
    });
});
