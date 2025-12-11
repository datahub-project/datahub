/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { ViewSelectDropdown } from '@app/entityV2/view/select/ViewSelectDropdown';

interface Props {
    children: React.ReactNode;
    privateView: boolean;
    publicView: boolean;
    onClickCreateView: () => void;
    onClickManageViews: () => void;
    onClickViewTypeFilter: (type: string) => void;
    onChangeSearch: (text: any) => void;
}

export const ViewSelectPopoverContent = ({
    privateView,
    publicView,
    onClickCreateView,
    onClickManageViews,
    onClickViewTypeFilter,
    onChangeSearch,
    children,
}: Props) => {
    return (
        <ViewSelectDropdown
            privateView={privateView}
            publicView={publicView}
            onClickCreateView={onClickCreateView}
            onClickManageViews={onClickManageViews}
            onClickViewTypeFilter={onClickViewTypeFilter}
            onChangeSearch={onChangeSearch}
        >
            {children}
        </ViewSelectDropdown>
    );
};
