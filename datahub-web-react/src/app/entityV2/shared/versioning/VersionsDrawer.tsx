import { CloseOutlined } from '@ant-design/icons';
import { useEntityContext } from '@app/entity/shared/EntityContext';
import { VersionPill } from '@app/entityV2/shared/versioning/common';
import { SimpleCopyLinkMenuItem } from '@app/shared/share/v2/items/CopyLinkMenuItem';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { colors, Icon, Input, Table, Text } from '@components';
import { Tooltip2 } from '@components/components/Tooltip2';
import { useSearchAcrossVersionsQuery } from '@graphql/versioning.generated';
import LinkOut from '@images/link-out.svg?react';
import { FilterOperator } from '@types';
import { Drawer, Dropdown, Pagination, Typography } from 'antd';
import { ItemType } from 'antd/es/menu/hooks/useItems';
import moment from 'moment';
import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { useDebounce } from 'react-use';
import styled from 'styled-components';

const PAGE_SIZE = 10;

const COLUMNS = [
    {
        title: 'Name',
        dataIndex: 'label',
        key: 'name',
    },
    {
        title: 'Note',
        dataIndex: 'comment',
        key: 'notes',
        width: '200px',
    },
    {
        title: 'Created',
        dataIndex: 'createdAt',
        key: 'created',
    },
    {
        title: '',
        dataIndex: 'menu',
        key: 'menu',
    },
];

const LinkOutIcon = styled(LinkOut)`
    width: 14px;
    height: 14px;
`;

const Contents = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 18px;
`;

const Title = styled(Text)`
    display: flex;
    align-items: center;
    justify-content: space-between;
`;

const CloseIcon = styled.div`
    color: ${colors.gray[1800]};
    cursor: pointer;
`;

const MenuIcon = styled(Icon)`
    border-radius: 200px;
    border: ${colors.gray[100]} 1px solid;
    cursor: pointer;
    transition: border 0.3s ease;

    svg {
        padding: 2px; // To match 3 dot icon in EntityDropdown
    }

    :hover {
        border: ${colors.violet[500]} 1px solid;
    }
`;

const MenuItemText = styled(Text)`
    display: flex;
    align-items: center;
    gap: 12px;
`;

const StyledDropdown = styled(Dropdown)`
    border-radius: 100px;
`;

interface Props {
    versionSetUrn: string;
    open: boolean;
}

export default function VersionsDrawer({ versionSetUrn, open }: Props) {
    const entityRegistry = useEntityRegistry();
    const { setDrawer } = useEntityContext();

    const [searchInput, setSearchInput] = useState<string>('');
    const [query, setQuery] = useState<string>('');
    const [page, setPage] = useState<number>(1);

    useDebounce(() => setQuery(searchInput), 200, [searchInput]);

    const { data } = useSearchAcrossVersionsQuery({
        skip: !open,
        variables: {
            versionSetUrn,
            input: {
                query: '*',
                count: PAGE_SIZE,
                start: (page - 1) * PAGE_SIZE,
                orFilters: [
                    {
                        and: [
                            {
                                field: 'version',
                                condition: FilterOperator.Contain,
                                values: [query],
                            }, // { field: 'sourceCreatedTimestamp', condition: FilterOperator.Between, values: query },
                        ],
                    },
                ],
            },
        },
    });

    const tableData = data?.versionSet?.versionsSearch?.searchResults?.map((version) => {
        const { urn, type } = version.entity;
        const versionProperties = entityRegistry.getGenericEntityProperties(type, version.entity)?.versionProperties;
        const items: ItemType[] = [
            {
                key: 'COPY',
                label: <SimpleCopyLinkMenuItem urn={urn} entityType={type} text="Copy Version Link" />,
            },
            {
                key: 'OPEN',
                label: (
                    <Link to={entityRegistry.getEntityUrl(type, urn)} onClick={() => setDrawer?.(undefined)}>
                        <MenuItemText>
                            <LinkOutIcon />
                            Open
                        </MenuItemText>
                    </Link>
                ),
            },
        ];

        return {
            urn,
            label: (
                <VersionPill
                    label={versionProperties?.version?.versionTag || '<unlabeled>'}
                    isLatest={versionProperties?.isLatest}
                />
            ),
            comment: <Typography.Text ellipsis={{ tooltip: true }}>{versionProperties?.comment}</Typography.Text>,
            createdAt: (
                <Tooltip2
                    width={250}
                    placement="topRight"
                    sections={[
                        {
                            title: 'Created in Source',
                            content: moment(versionProperties?.createdInSource?.time).format('MMMM D, YYYY h:mm A'),
                        },
                        {
                            title: 'Synced to DataHub',
                            content: moment(versionProperties?.created?.time).format('MMMM D, YYYY h:mm A'),
                        },
                    ]}
                >
                    {moment(versionProperties?.created?.time).fromNow()}
                </Tooltip2>
            ),
            menu: (
                <StyledDropdown
                    menu={{ items, style: { borderRadius: '12px', boxShadow: '0px 0px 14px 0px rgba(0, 0, 0, 0.15)' } }}
                    trigger={['click']}
                    overlayStyle={{ borderRadius: '100px' }}
                >
                    <MenuIcon icon="MoreVert" variant="outline" size="2xl" color="gray" />
                </StyledDropdown>
            ),
        };
    });

    return (
        <Drawer
            title={
                <Title size="xl" color="gray" colorLevel={600} weight="semiBold">
                    Versions
                    <CloseIcon onClick={() => setDrawer?.(undefined)}>
                        <CloseOutlined />
                    </CloseIcon>
                </Title>
            }
            open={open}
            width="542px"
            onClose={() => setDrawer?.(undefined)}
            closable={false}
        >
            <Contents>
                <Input
                    label=""
                    placeholder="Search versions by name..."
                    icon={{ name: 'MagnifyingGlass', source: 'phosphor' }}
                    value={searchInput}
                    setValue={setSearchInput}
                />
                <Table data={tableData || []} columns={COLUMNS} />
                <Pagination
                    pageSize={PAGE_SIZE}
                    current={page}
                    onChange={setPage}
                    total={data?.versionSet?.versionsSearch?.total}
                    hideOnSinglePage
                />
            </Contents>
        </Drawer>
    );
}
