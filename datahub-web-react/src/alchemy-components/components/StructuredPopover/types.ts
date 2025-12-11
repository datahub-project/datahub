/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

export interface SectionType {
    title: string | React.ReactNode;
    titleSuffix?: string | React.ReactNode;
    content: string | React.ReactNode;
}

export interface StructuredPopoverProps {
    header?: React.ComponentType;
    sections?: SectionType[];
    children?: React.ReactNode;
    width?: number;
}

export interface TooltipHeaderProps {
    title: string;
    titleSuffix?: React.ReactNode;
    subTitle?: string;
    image?: string;
    action?: React.ComponentType;
}
