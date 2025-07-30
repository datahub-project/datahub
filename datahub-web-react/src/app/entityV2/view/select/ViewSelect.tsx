import { Popover, colors } from '@components';
import React, { useRef } from 'react';
import { createPortal } from 'react-dom';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { ANTD_GRAY, REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { ViewBuilder } from '@app/entityV2/view/builder/ViewBuilder';
import { useViewsSelectContext } from '@app/entityV2/view/select/ViewSelectContext';
import { ViewSelectPopoverContent } from '@app/entityV2/view/select/ViewSelectPopoverContent';
import { renderSelectedView } from '@app/entityV2/view/select/renderSelectedView';
import { renderViewOptionGroup } from '@app/entityV2/view/select/renderViewOptionGroup';
import '@app/entityV2/view/select/style.css';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

const ViewSelectContainer = styled.div`
    &&& {
        display: flex;
        align-items: center;
        padding: 0px 0px;

        .ant-select {
            .ant-select-selection-search {
                position: absolute;
            }

            &.ant-select-open {
                .ant-select-selection-placeholder,
                .ant-select-selection-item {
                    color: ${ANTD_GRAY[1]};
                }
            }

            &:not(.ant-select-open) {
                .ant-select-selection-placeholder,
                .ant-select-selection-item {
                    color: #fff;
                }
            }

            .ant-select-selection-placeholder {
                display: flex;
                align-items: center;
                justify-content: center;
                height: 100%;
            }

            .ant-select-selection-item {
                font-weight: 700;
                font-size: 14px;
                text-align: right;
                padding: 0px;
            }
        }
    }
`;

const overlayInnerStyle = {
    background: 'transparent',
    display: 'flex',
    width: '100%',
};

const getOverlayInnerStyle = (isShowNavBarRedesign?: boolean) => {
    if (isShowNavBarRedesign)
        return {
            display: 'flex',
            width: '100%',
            opacity: 0.97,
            backgroundColor: colors.gray[1600],
            borderRadius: '0 0 12px 12px',
            paddingTop: '1px',
            boxShadow: '0px 525px 20px 500px rgba(0, 0, 0, 0.12), 0px 65px 60px 0px rgba(0, 0, 0, 0.12)',
        };

    return overlayInnerStyle;
};

const overlayStyle = {
    left: '0px',
    backgroundColor: REDESIGN_COLORS.BACKGROUND_OVERLAY_BLACK,
    backdropFilter: 'blur(5px)',
    opacity: 0.97,
    zIndex: 13,
    'transform-origin': '0',
};

const getOverlayStyle = (isShowNavBarRedesign?: boolean) => {
    if (isShowNavBarRedesign)
        return {
            left: '0px',
            zIndex: 13,
            paddingTop: '5px',
            'transform-origin': '0',
            width: '100%',
        };

    return overlayStyle;
};

const Blur = styled.div<{ $isOpen?: boolean }>`
    position: absolute;
    top: 69px;
    left: 0;
    width: 100%;
    height: calc(100vh - 69px);
    z-index: 12;
    backdrop-filter: blur(2px);
    ${(props) => !props.$isOpen && 'display: none;'}
`;

/**
 * The View Select component allows you to select a View to apply to query on the current page. For example,
 * search, recommendations, and browse.
 *
 * The current state of the View select includes an urn that must be forwarded with search, browse, and recommendations
 * requests. As we navigate around the app, the state of the selected View should not change.
 *
 * In the event that a user refreshes their browser, the state of the view should be saved as well.
 */
export const ViewSelect = () => {
    const userContext = useUserContext();

    const {
        isInternalOpen,
        onOpenChangeHandler,
        privateView,
        publicView,
        onClickCreateView,
        onClickManageViews,
        onClickViewTypeFilter,
        debouncedSetFilterText,
        hasViews,
        privateViewCount,
        publicViewCount,
        selectedUrn,
        highlightedPrivateViewData,
        highlightedPublicViewData,
        hoverViewUrn,
        scrollToRef,
        setHoverViewUrn,
        onClickEditView,
        onClickPreviewView,
        onClear,
        onSelectView,
        selectedViewName,
        viewBuilderDisplayState,
        onCloseViewBuilder,
    } = useViewsSelectContext();

    const isShowNavBarRedesign = useShowNavBarRedesign();

    const selectRef = useRef(null);

    return (
        <>
            {isShowNavBarRedesign && createPortal(<Blur $isOpen={isInternalOpen} />, document.body)}
            <ViewSelectContainer>
                <Popover
                    open={isInternalOpen}
                    onOpenChange={onOpenChangeHandler}
                    content={
                        <>
                            <ViewSelectPopoverContent
                                privateView={privateView}
                                publicView={publicView}
                                onClickCreateView={onClickCreateView}
                                onClickManageViews={onClickManageViews}
                                onClickViewTypeFilter={onClickViewTypeFilter}
                                onChangeSearch={debouncedSetFilterText}
                            >
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
                                        fixedWidth: true,
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
                                        fixedWidth: true,
                                        setHoverViewUrn,
                                        onClickEditView,
                                        onClickPreviewView,
                                        onClickClear: onClear,
                                        onSelectView,
                                    })}
                            </ViewSelectPopoverContent>
                        </>
                    }
                    trigger="click"
                    overlayClassName="view-select-popover"
                    overlayInnerStyle={getOverlayInnerStyle(isShowNavBarRedesign)}
                    overlayStyle={getOverlayStyle(isShowNavBarRedesign)}
                    showArrow={false}
                    popupVisible={false}
                    ref={selectRef}
                >
                    {renderSelectedView({ selectedViewName, onClear, isShowNavBarRedesign })}
                </Popover>
                {viewBuilderDisplayState.visible && (
                    <ViewBuilder
                        urn={viewBuilderDisplayState.view?.urn || undefined}
                        initialState={viewBuilderDisplayState.view}
                        mode={viewBuilderDisplayState.mode}
                        onSubmit={onCloseViewBuilder}
                        onCancel={onCloseViewBuilder}
                    />
                )}
            </ViewSelectContainer>
        </>
    );
};
