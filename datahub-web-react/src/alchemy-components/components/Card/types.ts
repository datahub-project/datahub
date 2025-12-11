/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { IconAlignmentOptions } from '@src/alchemy-components/theme/config';

export interface CardProps {
    title: string | React.ReactNode;
    subTitle?: string | React.ReactNode;
    percent?: number;
    button?: React.ReactNode;
    onClick?: () => void;
    icon?: React.ReactNode;
    iconAlignment?: IconAlignmentOptions;
    children?: React.ReactNode;
    width?: string;
    maxWidth?: string;
    height?: string;
    isEmpty?: boolean;
    style?: React.CSSProperties;
    isCardClickable?: boolean;
    dataTestId?: string;
}
