import React from 'react';
import { DataHubView } from '../../../../types.generated';
import { ViewOption } from './ViewOption';
import { UserContextType } from '../../../context/userContext';

const selectOptionStyle = { paddingLeft: 0 };

type Args = {
    selectedUrn: string | undefined;
    views: Array<DataHubView>;
    userContext: UserContextType;
    hoverViewUrn?: string;
    isOwnedByUser?: boolean;
    scrollToRef?: any;
    setHoverViewUrn: (viewUrn: string) => void;
    onClickEditView: (view: DataHubView) => void;
    onClickPreviewView: (view: DataHubView) => void;
    onClickClear: () => void;
    onSelectView: (newURn: string) => void;
};

export const renderViewOptionGroup = ({
    selectedUrn,
    views,
    userContext,
    hoverViewUrn,
    isOwnedByUser,
    scrollToRef,
    setHoverViewUrn,
    onClickEditView,
    onClickPreviewView,
    onClickClear,
    onSelectView,
}: Args) => {
    const maybePersonalDefaultViewUrn = userContext.state?.views?.personalDefaultViewUrn;
    const maybeGlobalDefaultViewUrn = userContext.state?.views?.globalDefaultViewUrn;

    return views.map((view) => (
        <div
            onMouseEnter={() => setHoverViewUrn(view.urn)}
            key={view.urn}
            style={selectOptionStyle}
            data-testid="view-select-item"
            onClick={() => onSelectView(view.urn)}
            role="none"
        >
            <ViewOption
                selectedUrn={selectedUrn === view.urn}
                scrollToRef={scrollToRef}
                view={view}
                showOptions={view.urn === hoverViewUrn}
                isOwnedByUser={isOwnedByUser}
                isUserDefault={view.urn === maybePersonalDefaultViewUrn}
                isGlobalDefault={view.urn === maybeGlobalDefaultViewUrn}
                onClickEdit={() => onClickEditView(view)}
                onClickPreview={() => onClickPreviewView(view)}
                onClickClear={onClickClear}
                selectView={() => onSelectView(view.urn)}
            />
        </div>
    ));
};
