import React from 'react';
import { ViewSelectDropdown } from './ViewSelectDropdown';

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
