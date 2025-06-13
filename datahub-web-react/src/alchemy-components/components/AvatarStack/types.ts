import { AvatarSizeOptions } from '@src/alchemy-components/theme/config';

import { EntityType } from '@types';

export interface AvatarItemProps {
    name: string;
    imageUrl?: string | null;
    type?: EntityType;
    urn?: string;
}

export type AvatarStackProps = {
    avatars: AvatarItemProps[];
    size?: AvatarSizeOptions;
    showRemainingNumber?: boolean;
    maxToShow?: number;
};
