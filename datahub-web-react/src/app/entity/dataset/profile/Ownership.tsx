import { AutoComplete, Avatar, Button, Col, Row, Select, Table, Typography } from 'antd';
import React, { useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { EntityType, Owner, OwnershipType, OwnershipUpdate } from '../../../../types.generated';
import defaultAvatar from '../../../../images/default_avatar.png';
import { useGetAutoCompleteResultsLazyQuery } from '../../../../graphql/search.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';

const OWNER_SEARCH_PLACEHOLDER = 'Enter an LDAP...';
const NUMBER_OWNERS_REQUIRED = 2;

interface Props {
    owners: Array<Owner>;
    lastModifiedAt: number;
    updateOwnership: (update: OwnershipUpdate) => void;
}

/**
 * Displays an array of owners! Work-in-progress.
 *
 * TODO: Add mutations to change ownership on explicit save.
 */
export const Ownership: React.FC<Props> = ({ owners, lastModifiedAt, updateOwnership }: Props): JSX.Element => {
    const entityRegistry = useEntityRegistry();

    const ownerTableData = useMemo(
        () =>
            owners.map((owner) => ({
                urn: owner.owner.urn,
                ldap: owner.owner.username,
                fullName: owner.owner.info?.fullName,
                type: 'USER',
                role: owner.type,
                pictureLink: owner.owner.editableInfo?.pictureLink,
            })),
        [owners],
    );

    const [showAddOwner, setShowAddOwner] = useState(false);
    const [ownerQuery, setOwnerQuery] = useState('');
    const [getOwnerAutoCompleteResults, { data: searchOwnerSuggestionsData }] = useGetAutoCompleteResultsLazyQuery();

    const onShowAddAnOwner = () => {
        setShowAddOwner(true);
    };

    const onDeleteOwner = (urn: string) => {
        const updatedOwners = owners
            .filter((owner) => !(owner.owner.urn === urn))
            .map((owner) => ({
                owner: owner.owner.urn,
                type: owner.type,
            }));
        updateOwnership({ owners: updatedOwners });
    };

    const onChangeOwnerQuery = (query: string) => {
        getOwnerAutoCompleteResults({
            variables: {
                input: {
                    type: EntityType.CorpUser,
                    query,
                    field: 'ldap',
                },
            },
        });
        setOwnerQuery(query);
    };

    const onSelectOwner = (ldap: string) => {
        const urn = `urn:li:corpuser:${ldap}`;
        const updatedOwners = [
            ...owners.map((owner) => ({ owner: owner.owner.urn, type: owner.type })),
            {
                owner: urn,
                type: OwnershipType.Stakeholder,
            },
        ];
        updateOwnership({ owners: updatedOwners });
        setOwnerQuery('');
    };

    const onChangeOwnershipType = (urn: string, type: OwnershipType) => {
        const updatedOwners = owners.map((owner) => ({
            owner: owner.owner.urn,
            type: owner.owner.urn === urn ? type : owner.type,
        }));
        updateOwnership({ owners: updatedOwners });
    };

    const ownerTableColumns = [
        {
            title: 'LDAP',
            dataIndex: 'ldap',
            render: (text: string, record: any) => (
                <Link to={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${record.urn}`}>
                    <Avatar
                        style={{
                            marginRight: '15px',
                            color: '#f56a00',
                            backgroundColor: '#fde3cf',
                        }}
                        src={record.pictureLink || defaultAvatar}
                    />
                    {text}
                </Link>
            ),
        },
        {
            title: 'Full Name',
            dataIndex: 'fullName',
        },
        {
            title: 'Type',
            dataIndex: 'type',
        },
        {
            title: 'Role',
            dataIndex: 'role',
            render: (role: OwnershipType, record: any) => (
                <Select defaultValue={role} onChange={() => onChangeOwnershipType(record.urn, role)}>
                    {Object.values(OwnershipType).map((value) => (
                        <Select.Option value={value}>{value}</Select.Option>
                    ))}
                </Select>
            ),
        },
        {
            title: '',
            key: 'action',
            render: (_: string, record: any) => (
                <Button type="link" style={{ color: 'red' }} onClick={() => onDeleteOwner(record.urn)}>
                    Remove
                </Button>
            ),
        },
    ];

    return (
        <div>
            <Row style={{ padding: '20px 0px 20px 0px' }}>
                <Col span={24}>
                    <Typography.Text style={{ float: 'right' }}>
                        Last updated <b>{new Date(lastModifiedAt).toLocaleDateString('en-US')}</b>
                    </Typography.Text>
                    <h1>Ownership</h1>
                    <div>
                        Please maintain at least <b>{NUMBER_OWNERS_REQUIRED}</b> owners.
                    </div>
                </Col>
            </Row>
            <Row>
                <Col span={24}>
                    <Table pagination={false} columns={ownerTableColumns} dataSource={ownerTableData} />
                </Col>
            </Row>
            <Row>
                <Col style={{ paddingTop: '15px' }} span={24}>
                    {!showAddOwner && (
                        <Button type="link" onClick={onShowAddAnOwner}>
                            <b> + </b> Add an owner
                        </Button>
                    )}
                    {showAddOwner && (
                        <AutoComplete
                            options={
                                (searchOwnerSuggestionsData &&
                                    searchOwnerSuggestionsData.autoComplete &&
                                    searchOwnerSuggestionsData.autoComplete.suggestions.map((suggestion: string) => ({
                                        value: suggestion,
                                    }))) ||
                                []
                            }
                            style={{
                                width: 150,
                            }}
                            value={ownerQuery}
                            onSelect={onSelectOwner}
                            onSearch={onChangeOwnerQuery}
                            placeholder={OWNER_SEARCH_PLACEHOLDER}
                        />
                    )}
                </Col>
            </Row>
        </div>
    );
};
