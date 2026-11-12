import { AvatarType } from '@components/components/AvatarStack/types';

import { EntityType } from '@types';

export const mapEntityTypeToAvatarType = (entityType: EntityType): AvatarType => {
    if (entityType === EntityType.CorpGroup) return AvatarType.group;
    return AvatarType.user;
};

export const mapAvatarTypeToEntityType = (avatarType: AvatarType): EntityType => {
    if (avatarType === AvatarType.group) return EntityType.CorpGroup;
    return EntityType.CorpUser;
};
