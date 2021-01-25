import { AutoComplete, Avatar, Button, Col, Row, Select, Table } from 'antd';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { Owner, OwnershipType } from '../../../types.generated';
import defaultAvatar from '../../../images/default_avatar.png';
import { PageRoutes } from '../../../conf/Global';

const OWNER_SEARCH_PLACEHOLDER = 'Enter an LDAP...';
const NUMBER_OWNERS_REQUIRED = 2;

interface Props {
    owners: Array<Owner>;
    lastModifiedAt: number;
}

/**
 * Displays an array of owners! Work-in-progress.
 *
 * TODO: Add mutations to change ownership on explicit save.
 */
export const Ownership: React.FC<Props> = ({ owners, lastModifiedAt }: Props): JSX.Element => {
    console.log(lastModifiedAt);

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

    const [ownerTableData, setOwnerTableData] = useState(getOwnerTableData(owners));
    const [showAddOwner, setShowAddOwner] = useState(false);
    const [ownerSuggestions, setOwnerSuggestions] = useState(new Array<string>());

    const onShowAddAnOwner = () => {
        setShowAddOwner(true);
    };

    const onDeleteOwner = (urn: string) => {
        const newOwnerTableData = ownerTableData.filter((ownerRow) => !(ownerRow.urn === urn));
        setOwnerTableData(newOwnerTableData);
    };

    const onOwnerQueryChange = (_: string) => {
        // TODO: Fetch real suggestions!
        setOwnerSuggestions(['jjoyce']);
    };

    const onSelectOwner = (ldap: string) => {
        // TODO: Remove sample suggestions.
        const newOwnerTableData = [
            ...ownerTableData,
            {
                urn: 'urn:li:corpuser:ldap',
                ldap,
                fullName: 'John Joyce',
                type: 'USER',
                role: OwnershipType.Delegate,
                pictureLink: null,
            },
        ];
        setOwnerTableData(newOwnerTableData);
    };

    const onOwnershipTypeChange = (urn: string, type: string) => {
        const newOwnerTableData = ownerTableData.map((ownerRow) =>
            ownerRow.urn === urn ? { ...ownerRow, type } : ownerRow,
        );
        setOwnerTableData(newOwnerTableData);
    };

    const ownerTableColumns = [
        {
            title: 'LDAP',
            dataIndex: 'ldap',
            render: (text: string, record: any) => (
                <Link to={`${PageRoutes.USERS}/${record.urn}`}>
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
                            options={ownerSuggestions.map((result: string) => ({ value: result }))}
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
