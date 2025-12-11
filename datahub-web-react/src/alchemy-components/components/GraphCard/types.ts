/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

export type GraphCardProps = {
    title: string;
    subTitle?: string | React.ReactNode;
    loading?: boolean;
    graphHeight?: string;
    width?: string;
    renderGraph: () => React.ReactNode;
    renderControls?: () => React.ReactNode;
    isEmpty?: boolean;
    emptyContent?: React.ReactNode;
    moreInfoModalContent?: React.ReactNode;
    showHeader?: boolean;
    showEmptyMessageHeader?: boolean;
    emptyMessage?: string;
    dataTestId?: string;
};
