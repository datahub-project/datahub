import { describe, expect, it } from 'vitest';

import { AvatarType } from '@components/components/AvatarStack/types';

import { mapAvatarTypeToEntityType, mapEntityTypeToAvatarType } from '@app/sharedV2/avatar/avatar.utils';

import { EntityType } from '@types';

describe('mapEntityTypeToAvatarType', () => {
    it('maps a group to the group avatar', () => {
        expect(mapEntityTypeToAvatarType(EntityType.CorpGroup)).toBe(AvatarType.group);
    });

    it('maps a user to the user avatar', () => {
        expect(mapEntityTypeToAvatarType(EntityType.CorpUser)).toBe(AvatarType.user);
    });

    it('falls back to the user avatar for any other entity type', () => {
        expect(mapEntityTypeToAvatarType(EntityType.Dataset)).toBe(AvatarType.user);
    });
});

describe('mapAvatarTypeToEntityType', () => {
    it('maps the group avatar to a group', () => {
        expect(mapAvatarTypeToEntityType(AvatarType.group)).toBe(EntityType.CorpGroup);
    });

    it('maps the user avatar to a user', () => {
        expect(mapAvatarTypeToEntityType(AvatarType.user)).toBe(EntityType.CorpUser);
    });

    it('falls back to a user for the role avatar', () => {
        expect(mapAvatarTypeToEntityType(AvatarType.role)).toBe(EntityType.CorpUser);
    });
});
