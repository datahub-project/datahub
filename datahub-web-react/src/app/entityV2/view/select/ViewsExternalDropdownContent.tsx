import { spacing } from '@components';
import React from 'react';
import styled from 'styled-components';

import DefaultDropdownContainer from '@components/components/Dropdown/DefaultDropdownContainer';

import { useUserContext } from '@app/context/useUserContext';
import { useViewsSelectContext } from '@app/entityV2/view/select/ViewSelectContext';
import CreateViewButton from '@app/entityV2/view/select/components/CreateViewButton';
import SearchBar from '@app/entityV2/view/select/components/SearchBar';
import ViewTypeSelect from '@app/entityV2/view/select/components/viewTypeSelect/ViewTypeSelect';
import { VIEW_CARD_MIN_WIDTH } from '@app/entityV2/view/select/constants';
import { renderViewOptionGroup } from '@app/entityV2/view/select/renderViewOptionGroup';

const Wrapper = styled.div`
    display: flex;

    // FYI: dropdown component adds only min-width that matches to width of children
    // These styles prevent stretching of dropdown because of its content
    min-width: inherit;
    width: 0px;
`;

const StyledDropdownContainer = styled(DefaultDropdownContainer)`
    display: flex;
`;

const DropdownContentHeader = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    gap: 18px;
    width: 100%;
`;

const DropdownContent = styled.div`
    padding-top: ${spacing.xsm};
    gap: ${spacing.xsm};
    height: 160px; // height to show two rows
    overflow-y: auto;
    overflow-x: hidden;

    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(${VIEW_CARD_MIN_WIDTH}px, 1fr));
`;

interface Props {
    className?: string;
}

export default function ViewsExternalDropdownContent({ className }: Props) {
    const userContext = useUserContext();

    const {
        hasViews,
        privateViewCount,
        privateView,
        selectedUrn,
        highlightedPrivateViewData,
        hoverViewUrn,
        setHoverViewUrn,
        onClickEditView,
        onClickPreviewView,
        onClear,
        onSelectView,
        publicViewCount,
        publicView,
        highlightedPublicViewData,
        scrollToRef,
        onClickCreateView,
        debouncedSetFilterText,
        onClickManageViews,
        onClickViewTypeFilter,
    } = useViewsSelectContext();

    return (
        <Wrapper className={className}>
            <StyledDropdownContainer>
                <DropdownContentHeader>
                    <SearchBar
                        onChangeSearch={debouncedSetFilterText}
                        onClickManageViews={onClickManageViews}
                        fullWidth
                    />
                    <ViewTypeSelect
                        privateViews={privateView}
                        publicViews={publicView}
                        onTypeSelect={onClickViewTypeFilter}
                        bordered
                        showV2
                    />
                </DropdownContentHeader>
                <DropdownContent>
                    <CreateViewButton onClick={onClickCreateView} />
                    {hasViews &&
                        privateViewCount > 0 &&
                        privateView &&
                        renderViewOptionGroup({
                            selectedUrn,
                            views: highlightedPrivateViewData,
                            isOwnedByUser: true,
                            userContext,
                            hoverViewUrn,
                            scrollToRef,
                            setHoverViewUrn,
                            onClickEditView,
                            onClickPreviewView,
                            onClickClear: onClear,
                            onSelectView,
                        })}
                    {hasViews &&
                        publicViewCount > 0 &&
                        publicView &&
                        renderViewOptionGroup({
                            selectedUrn,
                            views: highlightedPublicViewData,
                            userContext,
                            hoverViewUrn,
                            scrollToRef,
                            setHoverViewUrn,
                            onClickEditView,
                            onClickPreviewView,
                            onClickClear: onClear,
                            onSelectView,
                        })}
                </DropdownContent>
            </StyledDropdownContainer>
        </Wrapper>
    );
}
