import { AvatarSizeOptions } from '@src/alchemy-components/theme/config';

export interface AvatarItemProps {
    name: string;
    imageUrl?: string | null;
}

export type AvatarStackProps = {
    avatars: AvatarItemProps[];
    size?: AvatarSizeOptions;
};
