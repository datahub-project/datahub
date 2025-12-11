/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
    avatars?: AvatarItemProps[];
    size?: AvatarSizeOptions;
    showRemainingNumber?: boolean;
    maxToShow?: number;
    title?: string;
};
