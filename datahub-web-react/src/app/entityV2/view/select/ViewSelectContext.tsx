import { debounce } from 'lodash';
import React, { useCallback, useContext, useEffect, useRef, useState } from 'react';
import { useHistory } from 'react-router';

import { useUserContext } from '@app/context/useUserContext';
import { HALF_SECOND_IN_MS, MAX_ROWS_BEFORE_DEBOUNCE } from '@app/entityV2/shared/tabs/Dataset/Queries/utils/constants';
import { ViewBuilderMode } from '@app/entityV2/view/builder/types';
import { filterViews } from '@app/entityV2/view/select/utils';
import { DEFAULT_LIST_VIEWS_PAGE_SIZE } from '@app/entityV2/view/utils';
import { PageRoutes } from '@conf/Global';

import { useListGlobalViewsQuery, useListMyViewsQuery } from '@graphql/view.generated';
import { DataHubView, DataHubViewType } from '@types';

type ViewBuilderDisplayState = {
    mode: ViewBuilderMode;
    visible: boolean;
    view?: DataHubView;
};

export type ViewSelectContextType = {
    isInternalOpen: boolean;
    onOpenChangeHandler: () => void;
    privateView: boolean;
    publicView: boolean;
    onClickCreateView: () => void;
    onClickManageViews: () => void;
    onClickViewTypeFilter: (type: string) => void;
    debouncedSetFilterText: (event: React.ChangeEvent<HTMLInputElement>) => void;
    hasViews: boolean;
    privateViewCount: number;
    publicViewCount: number;
    selectedUrn: string | undefined;
    highlightedPrivateViewData: DataHubView[];
    highlightedPublicViewData: DataHubView[];
    hoverViewUrn: string | undefined;
    scrollToRef?: React.RefObject<HTMLDivElement>;
    setHoverViewUrn: (viewUrn: string) => void;
    onClickEditView: (view: DataHubView) => void;
    onClickPreviewView: (view: DataHubView) => void;
    onClear: () => void;
    onSelectView: (viewUrn: string) => void;
    selectedViewName: string;
    viewBuilderDisplayState: ViewBuilderDisplayState;
    onCloseViewBuilder: () => void;
    updateOpenState: (isOpen: boolean) => void;
    toggleOpenState: () => void;
};

const DEFAULT_CONTEXT: ViewSelectContextType = {
    isInternalOpen: false,
    onOpenChangeHandler: () => {},
    privateView: true,
    publicView: true,
    onClickCreateView: () => {},
    onClickManageViews: () => {},
    onClickViewTypeFilter: () => {},
    debouncedSetFilterText: () => {},

    hasViews: false,
    privateViewCount: 0,
    publicViewCount: 0,
    selectedUrn: undefined,
    highlightedPrivateViewData: [],
    highlightedPublicViewData: [],
    hoverViewUrn: undefined,
    setHoverViewUrn: () => {},
    onClickEditView: () => {},
    onClickPreviewView: () => {},
    onClear: () => {},
    onSelectView: () => {},
    selectedViewName: '',
    viewBuilderDisplayState: {
        mode: ViewBuilderMode.EDITOR,
        visible: false,
        view: undefined,
    },
    onCloseViewBuilder: () => {},
    updateOpenState: () => {},
    toggleOpenState: () => {},
};

export const ViewSelectContext = React.createContext<ViewSelectContextType>(DEFAULT_CONTEXT);

const DEFAULT_VIEW_BUILDER_DISPLAY_STATE = {
    mode: ViewBuilderMode.EDITOR,
    visible: false,
    view: undefined,
};

interface Props {
    isOpen?: boolean;
    onOpenChange?: (isOpen: boolean) => void;
}

export function useViewsSelectContext() {
    const context = useContext(ViewSelectContext);
    if (context === null)
        throw new Error(`${useViewsSelectContext.name} must be used under a ViewSelectContextProvider`);
    return context;
}

export default function ViewSelectContextProvider({ isOpen, onOpenChange, children }: React.PropsWithChildren<Props>) {
    const history = useHistory();
    const userContext = useUserContext();
    const [viewBuilderDisplayState, setViewBuilderDisplayState] = useState<ViewBuilderDisplayState>(
        DEFAULT_VIEW_BUILDER_DISPLAY_STATE,
    );
    const [selectedUrn, setSelectedUrn] = useState<string | undefined>(
        userContext.localState?.selectedViewUrn || undefined,
    );
    const [hoverViewUrn, setHoverViewUrn] = useState<string | undefined>(undefined);
    const [privateView, setPrivateView] = useState<boolean>(true);
    const [publicView, setPublicView] = useState<boolean>(true);

    const [filterText, setFilterText] = useState('');
    const [isInternalOpen, setIsInternalOpen] = useState(!!isOpen);
    const [selectedViewName, setSelectedView] = useState<string>('');

    const scrollToRef = useRef<HTMLDivElement>(null);
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

    // Possibility to control open/close state by parents components
    useEffect(() => {
        if (isOpen !== undefined) setIsInternalOpen(isOpen);
    }, [isOpen]);

    useEffect(() => {
        setSelectedUrn(userContext.localState?.selectedViewUrn || undefined);
        const selectedView =
            privateViewsData?.listMyViews?.views?.find(
                (view) => view?.urn === userContext.localState?.selectedViewUrn,
            ) ||
            publicViewsData?.listGlobalViews?.views?.find(
                (view) => view?.urn === userContext.localState?.selectedViewUrn,
            );
        if (selectedView === undefined) {
            setSelectedView('');
        } else {
            setSelectedView(selectedView.name);
        }
    }, [userContext.localState?.selectedViewUrn, setSelectedUrn, privateViewsData, publicViewsData]);

    const highlightedPublicViewData = filterViews(filterText, publicViewsData?.listGlobalViews?.views || []);
    const highlightedPrivateViewData = filterViews(filterText, privateViewsData?.listMyViews?.views || []);

    const debouncedSetFilterText = debounce(
        (e: React.ChangeEvent<HTMLInputElement>) => setFilterText(e.target.value),
        (highlightedPublicViewData.length || highlightedPrivateViewData.length) > MAX_ROWS_BEFORE_DEBOUNCE
            ? HALF_SECOND_IN_MS
            : 0,
    );

    /**
     * Event Handlers
     */

    const updateOpenState = useCallback(
        (newIsOpen: boolean) => {
            if (isOpen === undefined) setIsInternalOpen(newIsOpen);
            onOpenChange?.(newIsOpen);
        },
        [onOpenChange, isOpen],
    );

    const toggleOpenState = useCallback(() => {
        const newIsOpen = isOpen === undefined ? !isInternalOpen : !isOpen;
        updateOpenState(newIsOpen);
    }, [updateOpenState, isInternalOpen, isOpen]);

    const onSelectView = (newUrn) => {
        const selectedView =
            highlightedPrivateViewData?.find((view) => view?.urn === newUrn) ||
            highlightedPublicViewData?.find((view) => view?.urn === newUrn);
        setSelectedView(selectedView?.name ?? '');
        userContext.updateLocalState({
            ...userContext.localState,
            selectedViewUrn: newUrn,
        });
        setTimeout(() => {
            updateOpenState(false);
        }, 250);
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
        setSelectedView('');
        userContext.updateLocalState({
            ...userContext.localState,
            selectedViewUrn: undefined,
        });
        updateOpenState(false);
    };

    const onClickManageViews = () => {
        history.push(PageRoutes.SETTINGS_VIEWS);
        updateOpenState(false);
    };

    const onClickViewTypeFilter = (type: string) => {
        setPrivateView(type === 'private' || type === 'all');
        setPublicView(type === 'public' || type === 'all');
    };

    const onOpenChangeHandler = useCallback(() => {
        scrollToRef?.current?.scrollIntoView({ behavior: 'smooth', block: 'end' });
        updateOpenState(!isInternalOpen);
    }, [isInternalOpen, updateOpenState]);

    /**
     * Render variables
     */
    const privateViews = highlightedPrivateViewData || [];
    const publicViews = highlightedPublicViewData || [];
    const privateViewCount = privateViews?.length || 0;
    const publicViewCount = publicViews?.length || 0;
    const hasViews = privateViewCount > 0 || publicViewCount > 0 || false;

    return (
        <ViewSelectContext.Provider
            value={{
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
                updateOpenState,
                toggleOpenState,
            }}
        >
            {children}
        </ViewSelectContext.Provider>
    );
}
