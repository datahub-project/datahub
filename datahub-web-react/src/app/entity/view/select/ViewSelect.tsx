import React, { CSSProperties, useEffect, useRef, useState } from 'react';
import { useHistory } from 'react-router';
import { Select } from 'antd';
import styled from 'styled-components';
import { VscTriangleDown } from 'react-icons/vsc';
import { useListMyViewsQuery, useListGlobalViewsQuery } from '../../../../graphql/view.generated';
import { useUserContext } from '../../../context/useUserContext';
import { DataHubView, DataHubViewType } from '../../../../types.generated';
import { ViewBuilder } from '../builder/ViewBuilder';
import { DEFAULT_LIST_VIEWS_PAGE_SIZE } from '../utils';
import { PageRoutes } from '../../../../conf/Global';
import { ViewBuilderMode } from '../builder/types';
import { ViewSelectDropdown } from './ViewSelectDropdown';
import { renderViewOptionGroup } from './renderViewOptionGroup';
import { ANTD_GRAY_V2 } from '../../shared/constants';

type ViewBuilderDisplayState = {
    mode: ViewBuilderMode;
    visible: boolean;
    view?: DataHubView;
};

const TriangleIcon = styled(VscTriangleDown)<{ isOpen: boolean }>`
    color: ${(props) => (props.isOpen ? props.theme.styles['primary-color'] : ANTD_GRAY_V2[10])};
`;

const DEFAULT_VIEW_BUILDER_DISPLAY_STATE = {
    mode: ViewBuilderMode.EDITOR,
    visible: false,
    view: undefined,
};

const ViewSelectContainer = styled.div`
    &&& {
        display: flex;
        align-items: center;

        .ant-select {
            .ant-select-selection-search {
                position: absolute;
            }
            &.ant-select-open {
                .ant-select-selection-placeholder,
                .ant-select-selection-item {
                    color: ${(props) => props.theme.styles['primary-color']};
                }
            }
            &:not(.ant-select-open) {
                .ant-select-selection-placeholder,
                .ant-select-selection-item {
                    color: ${ANTD_GRAY_V2[10]};
                }
            }
            .ant-select-selection-placeholder,
            .ant-select-selection-item {
                font-weight: 700;
                font-size: 14px;
                text-align: left;
            }
        }
    }
`;

const SelectStyled = styled(Select)`
    min-width: 90px;
    max-width: 200px;
`;

type Props = {
    dropdownStyle?: CSSProperties;
};

/**
 * The View Select component allows you to select a View to apply to query on the current page. For example,
 * search, recommendations, and browse.
 *
 * The current state of the View select includes an urn that must be forwarded with search, browse, and recommendations
 * requests. As we navigate around the app, the state of the selected View should not change.
 *
 * In the event that a user refreshes their browser, the state of the view should be saved as well.
 */
export const ViewSelect = ({ dropdownStyle = {} }: Props) => {
    const history = useHistory();
    const userContext = useUserContext();
    const [isOpen, setIsOpen] = useState(false);
    const [viewBuilderDisplayState, setViewBuilderDisplayState] = useState<ViewBuilderDisplayState>(
        DEFAULT_VIEW_BUILDER_DISPLAY_STATE,
    );
    const [selectedUrn, setSelectedUrn] = useState<string | undefined>(
        userContext.localState?.selectedViewUrn || undefined,
    );
    const [hoverViewUrn, setHoverViewUrn] = useState<string | undefined>(undefined);

    useEffect(() => {
        setSelectedUrn(userContext.localState?.selectedViewUrn || undefined);
    }, [userContext.localState?.selectedViewUrn, setSelectedUrn]);

    const selectRef = useRef(null);

    /**
     * Queries - Notice, each of these queries is cached. Here we fetch both the user's private views,
     * along with all public views.
     */

    const { data: privateViewsData } = useListMyViewsQuery({
        variables: {
            start: 0,
            count: DEFAULT_LIST_VIEWS_PAGE_SIZE,
            viewType: DataHubViewType.Personal,
        },
        fetchPolicy: 'cache-first',
    });

    // Fetch Public Views
    const { data: publicViewsData } = useListGlobalViewsQuery({
        variables: {
            start: 0,
            count: DEFAULT_LIST_VIEWS_PAGE_SIZE,
        },
        fetchPolicy: 'cache-first',
    });

    /**
     * Event Handlers
     */

    const onSelectView = (newUrn) => {
        userContext.updateLocalState({
            ...userContext.localState,
            selectedViewUrn: newUrn,
        });
    };

    const onClickCreateView = () => {
        setViewBuilderDisplayState({
            visible: true,
            mode: ViewBuilderMode.EDITOR,
            view: undefined,
        });
    };

    const onClickEditView = (view) => {
        setViewBuilderDisplayState({
            visible: true,
            mode: ViewBuilderMode.EDITOR,
            view,
        });
    };

    const onCloseViewBuilder = () => {
        setViewBuilderDisplayState(DEFAULT_VIEW_BUILDER_DISPLAY_STATE);
    };

    const onClickPreviewView = (view) => {
        setViewBuilderDisplayState({
            visible: true,
            mode: ViewBuilderMode.PREVIEW,
            view,
        });
    };

    const onClear = () => {
        setSelectedUrn(undefined);
        userContext.updateLocalState({
            ...userContext.localState,
            selectedViewUrn: undefined,
        });
    };

    const onClickManageViews = () => {
        history.push(PageRoutes.SETTINGS_VIEWS);
    };

    /**
     * Render variables
     */
    const privateViews = privateViewsData?.listMyViews?.views || [];
    const publicViews = publicViewsData?.listGlobalViews?.views || [];
    const privateViewCount = privateViews?.length || 0;
    const publicViewCount = publicViews?.length || 0;
    const hasViews = privateViewCount > 0 || publicViewCount > 0 || false;

    /**
     * Notice - we assume that we will find the selected View urn in the list
     * of Views retrieved for the user (private or public). If this is not the case,
     * the view will be set to undefined.
     *
     * This may become a problem if a list of public views exceeds the default pagination size of 1,000.
     */
    const foundSelectedUrn =
        (privateViews.filter((view) => view.urn === selectedUrn)?.length || 0) > 0 ||
        (publicViews.filter((view) => view.urn === selectedUrn)?.length || 0) > 0 ||
        false;

    const handleDropdownVisibleChange = (isNowOpen: boolean) => {
        setIsOpen(isNowOpen);
    };

    return (
        <ViewSelectContainer>
            <SelectStyled
                data-testid="view-select"
                onChange={() => (selectRef?.current as any)?.blur()}
                value={(foundSelectedUrn && selectedUrn) || undefined}
                placeholder="View all"
                onSelect={onSelectView}
                onClear={onClear}
                ref={selectRef}
                optionLabelProp="label"
                bordered={false}
                dropdownMatchSelectWidth={false}
                suffixIcon={<TriangleIcon isOpen={isOpen} />}
                dropdownStyle={{
                    paddingBottom: 0,
                    ...dropdownStyle,
                }}
                onDropdownVisibleChange={handleDropdownVisibleChange}
                dropdownRender={(menu) => (
                    <ViewSelectDropdown
                        menu={menu}
                        hasViews={hasViews}
                        onClickCreateView={onClickCreateView}
                        onClickClear={onClear}
                        onClickManageViews={onClickManageViews}
                    />
                )}
            >
                {privateViewCount > 0 &&
                    renderViewOptionGroup({
                        views: privateViews,
                        label: 'Private',
                        isOwnedByUser: true,
                        userContext,
                        hoverViewUrn,
                        setHoverViewUrn,
                        onClickEditView,
                        onClickPreviewView,
                    })}
                {publicViewCount > 0 &&
                    renderViewOptionGroup({
                        views: publicViews,
                        label: 'Public',
                        userContext,
                        hoverViewUrn,
                        setHoverViewUrn,
                        onClickEditView,
                        onClickPreviewView,
                    })}
            </SelectStyled>
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
    );
};
