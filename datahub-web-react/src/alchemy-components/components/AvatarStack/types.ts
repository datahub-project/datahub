import { AvatarSizeOptions } from '@src/alchemy-components/theme/config';

export enum AvatarType {
    user,
    group,
    role,
}
export interface AvatarItemProps {
    name: string;
    imageUrl?: string | null;
    urn?: string;
    type?: AvatarType;
}

export type AvatarStackProps = {
    avatars: AvatarItemProps[];
    size?: AvatarSizeOptions;
    showRemainingNumber?: boolean;
    maxToShow?: number;
    title?: string;
};
