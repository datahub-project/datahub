import React from 'react';
import { ViewSelectFooter } from './ViewSelectFooter';
import { ViewSelectHeader } from './ViewSelectHeader';

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
            <ViewSelectHeader onClickCreateView={onClickCreateView} onClickClear={onClickClear} />
            {hasViews && menu}
            <ViewSelectFooter onClickManageViews={onClickManageViews} />
        </>
    );
};
