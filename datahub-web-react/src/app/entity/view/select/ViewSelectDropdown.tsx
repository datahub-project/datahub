/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { ViewSelectFooter } from '@app/entity/view/select/ViewSelectFooter';
import { ViewSelectHeader } from '@app/entity/view/select/ViewSelectHeader';

type Props = {
    menu: React.ReactNode;
    hasViews: boolean;
    onClickCreateView: () => void;
    onClickManageViews: () => void;
    onClickClear: () => void;
};

export const ViewSelectDropdown = ({ menu, hasViews, onClickCreateView, onClickManageViews, onClickClear }: Props) => {
    return (
        <>
            <ViewSelectHeader onClickClear={onClickClear} />
            {hasViews && menu}
            <ViewSelectFooter
                hasViews={hasViews}
                onClickCreateView={onClickCreateView}
                onClickManageViews={onClickManageViews}
            />
        </>
    );
};
