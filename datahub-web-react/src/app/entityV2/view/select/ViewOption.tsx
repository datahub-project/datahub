import React from 'react';
import styled from 'styled-components';
import CloseIcon from '@mui/icons-material/Close';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { DataHubView } from '../../../../types.generated';
import { ViewOptionName } from './ViewOptionName';
import { ANTD_GRAY, REDESIGN_COLORS, SEARCH_COLORS } from '../../shared/constants';

const Container = styled.div`
    display: flex;
    width: 100%;
    gap: 0.5rem;
`;

const ViewDetailsContainer = styled.div<{ selected: boolean; $isShowNavBarRedesign?: boolean }>`
    display: flex;
    align-items: center;
    position: relative;
    background: ${(props) => (props.selected ? SEARCH_COLORS.TITLE_PURPLE : '')};
    ${(props) => !props.$isShowNavBarRedesign && 'padding: 10px;'}
    border-radius: 16px;
    ${(props) =>
        !props.$isShowNavBarRedesign &&
        `
        border: 1px solid ${props.selected ? SEARCH_COLORS.TITLE_PURPLE : REDESIGN_COLORS.BORDER_1};
    `}

    &:hover {
        ${(props) =>
            !props.$isShowNavBarRedesign &&
            `
            border: ${`1px solid ${SEARCH_COLORS.TITLE_PURPLE}`};
            padding: 10px;
        `}
        border-radius: 16px;
        & .create-view-icon {
            background: ${SEARCH_COLORS.TITLE_PURPLE} !important;
            border: ${(props) => (!props.selected ? `1px solid ${SEARCH_COLORS.TITLE_PURPLE} !important` : '')};
        }
    }
    & .default-view-icon-container {
        border: 1px solid
            ${(props) => (props.selected ? SEARCH_COLORS.TITLE_PURPLE : REDESIGN_COLORS.BACKGROUND_OVERLAY_BLACK)};
        border-radius: 100%;
    }
    & .close-container {
        position: absolute;
        top: -10px;
        right: -5px;
        background-color: ${ANTD_GRAY[1]};
        display: flex;
        align-items: center;
        border-radius: 100%;
        padding: 5px;
        cursor: pointer;
    }
`;

const CloseIconStyle = styled(CloseIcon)`
    font-size: 14px !important;
    color: ${SEARCH_COLORS.TITLE_PURPLE};
`;

type Props = {
    selectedUrn: boolean;
    view: DataHubView;
    showOptions: boolean;
    isGlobalDefault: boolean;
    isUserDefault: boolean;
    isOwnedByUser?: boolean;
    scrollToRef?: any;
    onClickEdit: () => void;
    onClickPreview: () => void;
    onClickClear: () => void;
    selectView: () => void;
};

export const ViewOption = ({
    selectedUrn,
    view,
    showOptions,
    isGlobalDefault,
    isUserDefault,
    isOwnedByUser,
    scrollToRef,
    onClickEdit,
    onClickPreview,
    onClickClear,
    selectView,
}: Props) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const onClear = (e) => {
        e.stopPropagation();
        onClickClear();
    };

    return (
        <Container>
            <ViewDetailsContainer
                selected={selectedUrn}
                ref={selectedUrn ? scrollToRef : null}
                $isShowNavBarRedesign={isShowNavBarRedesign}
            >
                <ViewOptionName
                    name={view.name}
                    description={view.description}
                    type={view.viewType}
                    isUserDefault={isUserDefault}
                    isGlobalDefault={isGlobalDefault}
                    view={view}
                    isOwnedByUser={isOwnedByUser}
                    visible={showOptions}
                    onClickEdit={onClickEdit}
                    onClickPreview={onClickPreview}
                    selected={selectedUrn}
                    selectView={selectView}
                />
                {selectedUrn && (
                    <div className="close-container" onClick={(e) => onClear(e)} role="none">
                        <CloseIconStyle />
                    </div>
                )}
            </ViewDetailsContainer>
        </Container>
    );
};
