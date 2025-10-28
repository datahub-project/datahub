import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { ANTD_GRAY } from '@app/entityV2/shared/constants';
import { ViewTypeLabel } from '@app/entityV2/view/ViewTypeLabel';
import { ViewDropdownMenu } from '@app/entityV2/view/menu/ViewDropdownMenu';
import { GlobalDefaultViewIcon } from '@app/entityV2/view/shared/GlobalDefaultViewIcon';
import { UserDefaultViewIcon } from '@app/entityV2/view/shared/UserDefaultViewIcon';

import { DataHubViewType } from '@types';

const StyledDescription = styled.div`
    max-width: 300px;
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
    onEditView?: (urn) => void;
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
            <Text size="md" weight="semiBold" onClick={() => onEditView?.(record.urn)}>
                {name}
            </Text>
        </NameContainer>
    );
}

type DescriptionColumnProps = {
    description: string;
};

export function DescriptionColumn({ description }: DescriptionColumnProps) {
    return <StyledDescription>{description || '-'}</StyledDescription>;
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
    return <ViewDropdownMenu view={record} visible />;
}
