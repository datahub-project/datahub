/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

export interface IconLabelProps {
    icon: JSX.Element;
    name: string;
    type: IconType;
    marginRight?: string;
    imageUrl?: string;
    style?: React.CSSProperties;
    testId?: string;
}

export enum IconType {
    ICON = 'ICON',
    IMAGE = 'IMAGE',
}
