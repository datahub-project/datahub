import { Tooltip } from '@components';
import { Tag, Typography } from 'antd';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

import DomainItemMenu from '@app/domainV2/DomainItemMenu';
import { OwnerAvatarGroup } from '@app/sharedV2/owners/OwnerAvatarGroup';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { Maybe, Ownership } from '@types';

interface DomainEntry {
    name: string;
    entities: string;
    urn: string;
    ownership?: Maybe<Ownership>;
    url: string;
}

const AvatarGroupWrapper = styled.div`
    margin-right: 10px;
    display: inline-block;
`;

const DomainNameContainer = styled.div`
    margin-left: 16px;
    margin-right: 16px;
    display: inline;
`;

export function DomainListMenuColumn(handleDelete: (urn: string) => void) {
    return (record: DomainEntry) => (
        <DomainItemMenu name={record.name} urn={record.urn} onDelete={() => handleDelete(record.urn)} />
    );
}

export function DomainNameColumn(logoIcon: JSX.Element) {
    return (record: DomainEntry) => (
        <span data-testid={record.urn}>
            <Link to={record.url}>
                {logoIcon}
                <DomainNameContainer>
                    <Typography.Text>{record.name}</Typography.Text>
                </DomainNameContainer>
                <Tooltip title={`There are ${record.entities} entities in this domain.`}>
                    <Tag>{record.entities} entities</Tag>
                </Tooltip>
            </Link>
        </span>
    );
}

export function DomainOwnersColumn(ownership: Maybe<Ownership>) {
    const entityRegistry = useEntityRegistryV2();

    if (!ownership) {
        return null;
    }

    const { owners } = ownership;
    if (!owners || owners.length === 0) {
        return null;
    }

    return (
        <AvatarGroupWrapper>
            <OwnerAvatarGroup owners={owners} entityRegistry={entityRegistry} />
        </AvatarGroupWrapper>
    );
}
