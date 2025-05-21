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
