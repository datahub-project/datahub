import { AvatarSizeOptions } from '@src/alchemy-components/theme/config';

export interface AvatarItemProps {
    name: string;
    imageUrl?: string | null;
}

export type AvatarListProps = {
    avatars: AvatarItemProps[];
    size?: AvatarSizeOptions;
};
