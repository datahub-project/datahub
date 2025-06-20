import { Button, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { ANTD_GRAY } from '@app/entity/shared/constants';
import { ViewTypeLabel } from '@app/entity/view/ViewTypeLabel';
import { ViewDropdownMenu } from '@app/entity/view/menu/ViewDropdownMenu';
import { GlobalDefaultViewIcon } from '@app/entity/view/shared/GlobalDefaultViewIcon';
import { UserDefaultViewIcon } from '@app/entity/view/shared/UserDefaultViewIcon';

import { DataHubViewType } from '@types';

const StyledDescription = styled.div`
    max-width: 300px;
`;

const ActionButtonsContainer = styled.div`
    display: flex;
    justify-content: center;
    align-items: center;
    padding-right: 8px;
`;

const NameContainer = styled.span`
    display: flex;
    align-items: center;
    justify-content: left;
`;

const IconPlaceholder = styled.span`
    display: flex;
    align-items: center;
    justify-content: center;
`;

type NameColumnProps = {
    name: string;
    record: any;
    onEditView: (urn) => void;
};

export function NameColumn({ name, record, onEditView }: NameColumnProps) {
    const userContext = useUserContext();
    const maybePersonalDefaultViewUrn = userContext.state?.views?.personalDefaultViewUrn;
    const maybeGlobalDefaultViewUrn = userContext.state?.views?.globalDefaultViewUrn;

    const isUserDefault = record.urn === maybePersonalDefaultViewUrn;
    const isGlobalDefault = record.urn === maybeGlobalDefaultViewUrn;

    return (
        <NameContainer>
            <IconPlaceholder>
                {isUserDefault && <UserDefaultViewIcon title="Your default View." />}
                {isGlobalDefault && <GlobalDefaultViewIcon title="Your organization's default View." />}
            </IconPlaceholder>
            <Button type="text" onClick={() => onEditView(record.urn)}>
                <Typography.Text strong>{name}</Typography.Text>
            </Button>
        </NameContainer>
    );
}

type DescriptionColumnProps = {
    description: string;
};

export function DescriptionColumn({ description }: DescriptionColumnProps) {
    return (
        <StyledDescription>
            {description || <Typography.Text type="secondary">No description</Typography.Text>}
        </StyledDescription>
    );
}

type ViewTypeColumnProps = {
    viewType: DataHubViewType;
};

export function ViewTypeColumn({ viewType }: ViewTypeColumnProps) {
    return <ViewTypeLabel color={ANTD_GRAY[8]} type={viewType} />;
}

type ActionColumnProps = {
    record: any;
};

export function ActionsColumn({ record }: ActionColumnProps) {
    return (
        <ActionButtonsContainer>
            <ViewDropdownMenu view={record} visible />
        </ActionButtonsContainer>
    );
}
