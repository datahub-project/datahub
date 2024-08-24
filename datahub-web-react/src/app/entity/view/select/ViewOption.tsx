import React from 'react';
import styled from 'styled-components';
import { DataHubView } from '../../../../types.generated';
import { ViewOptionName } from './ViewOptionName';
import { ViewDropdownMenu } from '../menu/ViewDropdownMenu';
import { UserDefaultViewIcon } from '../shared/UserDefaultViewIcon';
import { GlobalDefaultViewIcon } from '../shared/GlobalDefaultViewIcon';

const ICON_WIDTH = 30;

const Container = styled.div`
    display: flex;
    align-items: center;
    justify-content: stretch;
    width: 100%;
`;

const IconPlaceholder = styled.div`
    width: ${ICON_WIDTH}px;
    display: flex;
    align-items: center;
    justify-content: center;
`;

type Props = {
    view: DataHubView;
    showOptions: boolean;
    isGlobalDefault: boolean;
    isUserDefault: boolean;
    isOwnedByUser?: boolean;
    onClickEdit: () => void;
    onClickPreview: () => void;
};

export const ViewOption = ({
    view,
    showOptions,
    isGlobalDefault,
    isUserDefault,
    isOwnedByUser,
    onClickEdit,
    onClickPreview,
}: Props) => {
    return (
        <Container>
            <IconPlaceholder>
                {isUserDefault && <UserDefaultViewIcon title="Your default View." />}
                {isGlobalDefault && <GlobalDefaultViewIcon title="Your organization's default View." />}
            </IconPlaceholder>
            <ViewOptionName name={view.name} description={view.description} />
            <ViewDropdownMenu
                view={view}
                isOwnedByUser={isOwnedByUser}
                visible={showOptions}
                onClickEdit={onClickEdit}
                onClickPreview={onClickPreview}
            />
        </Container>
    );
};
