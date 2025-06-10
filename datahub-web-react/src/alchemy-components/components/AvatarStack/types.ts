import { AvatarSizeOptions } from '@src/alchemy-components/theme/config';

export enum AvatarType {
    user,
    group,
}
export interface AvatarItemProps {
    name: string;
    imageUrl?: string | null;
    type?: AvatarType;
}

export type AvatarStackProps = {
    avatars: AvatarItemProps[];
    size?: AvatarSizeOptions;
    showRemainingNumber?: boolean;
    maxToShow?: number;
};
