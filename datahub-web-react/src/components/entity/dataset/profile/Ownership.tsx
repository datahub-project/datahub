import { AutoComplete, Avatar, Button, Col, Row, Select, Table } from 'antd';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { EntityType, Owner, OwnershipType } from '../../../../types.generated';
import defaultAvatar from '../../../../images/default_avatar.png';
import { useGetAutoCompleteResultsLazyQuery } from '../../../../graphql/search.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';

const OWNER_SEARCH_PLACEHOLDER = 'Enter an LDAP...';
const NUMBER_OWNERS_REQUIRED = 2;

interface Props {
    initialOwners: Array<Owner>;
    lastModifiedAt: number;
}

/**
 * Displays an array of owners! Work-in-progress.
 *
 * TODO: Add mutations to change ownership on explicit save.
 */
export const Ownership: React.FC<Props> = ({
    initialOwners: _initialOwners,
    lastModifiedAt: _lastModifiedAt,
}: Props): JSX.Element => {
    console.log(_lastModifiedAt);

    const entityRegistry = useEntityRegistry();

    const getOwnerTableData = (ownerArr: Array<Owner>) => {
        const rows = ownerArr.map((owner) => ({
            urn: owner.owner.urn,
            ldap: owner.owner.username,
            fullName: owner.owner.info?.fullName,
            type: 'USER',
            role: owner.type,
            pictureLink: owner.owner.editableInfo?.pictureLink,
        }));
        return rows;
    };

    const [owners, setOwners] = useState(_initialOwners);
    const [showAddOwner, setShowAddOwner] = useState(false);
    const [getOwnerAutoCompleteResults, { data: searchOwnerSuggestionsData }] = useGetAutoCompleteResultsLazyQuery();

    const onShowAddAnOwner = () => {
        setShowAddOwner(true);
    };

    const onDeleteOwner = (urn: string) => {
        const newOwners = owners.filter((owner: Owner) => !(owner.owner.urn === urn));
        setOwners(newOwners);
    };

    const onOwnerQueryChange = (query: string) => {
        getOwnerAutoCompleteResults({
            variables: {
                input: {
                    type: EntityType.User,
                    query,
                    field: 'ldap',
                },
            },
        });
    };

    const onSelectOwner = (ldap: string) => {
        // TODO: Remove this sample code.
        const newOwners = [
            ...owners,
            {
                owner: {
                    urn: `urn:li:corpuser:${ldap}`,
                    username: ldap,
                    info: {
                        fullName: 'John Joyce',
                    },
                    editableInfo: {
                        pictureLink: null,
                    },
                },
                type: OwnershipType.Delegate,
            } as Owner,
        ];
        setOwners(newOwners);
    };

    const onOwnershipTypeChange = (urn: string, type: OwnershipType) => {
        const newOwners = owners.map((owner: Owner) => (owner.owner.urn === urn ? { ...owner, type } : owner));
        setOwners(newOwners);
    };

    const ownerTableColumns = [
        {
            title: 'LDAP',
            dataIndex: 'ldap',
            render: (text: string, record: any) => (
                <Link to={`${entityRegistry.getPathName(EntityType.User)}/${record.urn}`}>
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
                <Select defaultValue={role} onChange={() => onOwnershipTypeChange(record.urn, role)}>
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
                    <p style={{ float: 'right' }}>
                        Last updated <b>2 days ago</b>
                    </p>
                    <h1>Ownership</h1>
                    <div>
                        Please maintain at least <b>{NUMBER_OWNERS_REQUIRED}</b> owners.
                    </div>
                </Col>
            </Row>
            <Row>
                <Col span={24}>
                    <Table pagination={false} columns={ownerTableColumns} dataSource={getOwnerTableData(owners)} />
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
                                    searchOwnerSuggestionsData.autoComplete.suggestions.map((result: string) => ({
                                        value: result,
                                    }))) ||
                                []
                            }
                            style={{
                                width: 150,
                            }}
                            onSelect={onSelectOwner}
                            onSearch={onOwnerQueryChange}
                            placeholder={OWNER_SEARCH_PLACEHOLDER}
                        />
                    )}
                </Col>
            </Row>
        </div>
    );
};
