import React from 'react';
import { Select } from 'antd';
import { DataHubView } from '../../../../types.generated';
import { ViewOption } from './ViewOption';
import { UserContextType } from '../../../context/userContext';

const selectOptionStyle = { paddingLeft: 0 };

type Args = {
    views: Array<DataHubView>;
    label: string;
    userContext: UserContextType;
    hoverViewUrn?: string;
    isOwnedByUser?: boolean;
    setHoverViewUrn: (viewUrn: string) => void;
    onClickEditView: (view: DataHubView) => void;
    onClickPreviewView: (view: DataHubView) => void;
};

export const renderViewOptionGroup = ({
    views,
    label,
    userContext,
    hoverViewUrn,
    isOwnedByUser,
    setHoverViewUrn,
    onClickEditView,
    onClickPreviewView,
}: Args) => {
    const maybePersonalDefaultViewUrn = userContext.state?.views?.personalDefaultViewUrn;
    const maybeGlobalDefaultViewUrn = userContext.state?.views?.globalDefaultViewUrn;

    return (
        <Select.OptGroup label={label} key={label}>
            {views.map((view) => (
                <Select.Option
                    onMouseEnter={() => setHoverViewUrn(view.urn)}
                    key={view.urn}
                    label={view.name}
                    value={view.urn}
                    style={selectOptionStyle}
                    data-testid="view-select-item"
                >
                    <ViewOption
                        view={view}
                        showOptions={view.urn === hoverViewUrn}
                        isOwnedByUser={isOwnedByUser}
                        isUserDefault={view.urn === maybePersonalDefaultViewUrn}
                        isGlobalDefault={view.urn === maybeGlobalDefaultViewUrn}
                        onClickEdit={() => onClickEditView(view)}
                        onClickPreview={() => onClickPreviewView(view)}
                    />
                </Select.Option>
            ))}
        </Select.OptGroup>
    );
};
