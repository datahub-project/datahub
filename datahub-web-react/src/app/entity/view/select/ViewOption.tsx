/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { ViewDropdownMenu } from '@app/entity/view/menu/ViewDropdownMenu';
import { ViewOptionName } from '@app/entity/view/select/ViewOptionName';
import { GlobalDefaultViewIcon } from '@app/entity/view/shared/GlobalDefaultViewIcon';
import { UserDefaultViewIcon } from '@app/entity/view/shared/UserDefaultViewIcon';

import { DataHubView } from '@types';

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
