import { CloseOutlined } from '@ant-design/icons';
import { Icon, Input, Table, Text } from '@components';
import { DotsThreeVertical } from '@phosphor-icons/react/dist/csr/DotsThreeVertical';
import { MagnifyingGlass } from '@phosphor-icons/react/dist/csr/MagnifyingGlass';
import { Drawer, Dropdown, Pagination, Typography } from 'antd';
import { ItemType } from 'antd/es/menu/hooks/useItems';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router-dom';
import { useDebounce } from 'react-use';
import styled, { useTheme } from 'styled-components';

import { StructuredPopover } from '@components/components/StructuredPopover';

import { useEntityContext } from '@app/entity/shared/EntityContext';
import { VersionPill } from '@app/entityV2/shared/versioning/common';
import { SimpleCopyLinkMenuItem } from '@app/shared/share/v2/items/CopyLinkMenuItem';
import { useEntityRegistry } from '@app/useEntityRegistry';
import dayjs from '@utils/dayjs';

import { useSearchAcrossVersionsQuery } from '@graphql/versioning.generated';
import { FilterOperator } from '@types';

import LinkOut from '@images/link-out.svg?react';

const PAGE_SIZE = 10;
const TIMESTAMP_FORMAT = 'MMMM D, YYYY h:mm A';

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
    color: ${(props) => props.theme.colors.textTertiary};
    cursor: pointer;
`;

const MenuIcon = styled(Icon)`
    border-radius: 200px;
    border: ${(props) => props.theme.colors.border} 1px solid;
    cursor: pointer;
    transition: border 0.3s ease;

    svg {
        padding: 2px; // To match 3 dot icon in EntityDropdown
    }

    :hover {
        border: ${(props) => props.theme.colors.borderBrand} 1px solid;
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
    const { t } = useTranslation('entity.shared.versioning');
    const { t: tc } = useTranslation('common.actions');
    const { t: tcl } = useTranslation('common.labels');
    const entityRegistry = useEntityRegistry();
    const theme = useTheme();
    const { setDrawer } = useEntityContext();

    const columns = [
        {
            title: tcl('name'),
            dataIndex: 'label',
            key: 'name',
        },
        {
            title: t('noteColumn'),
            dataIndex: 'comment',
            key: 'notes',
            width: '200px',
        },
        {
            title: tcl('created'),
            dataIndex: 'createdAt',
            key: 'created',
        },
        {
            title: '',
            dataIndex: 'menu',
            key: 'menu',
        },
    ];

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
                label: <SimpleCopyLinkMenuItem urn={urn} entityType={type} text={t('copyVersionLink')} />,
            },
            {
                key: 'OPEN',
                label: (
                    <Link to={entityRegistry.getEntityUrl(type, urn)} onClick={() => setDrawer?.(undefined)}>
                        <MenuItemText>
                            <LinkOutIcon />
                            {tc('open')}
                        </MenuItemText>
                    </Link>
                ),
            },
        ];

        return {
            urn,
            label: (
                /* eslint-disable i18next/no-literal-string -- (untranslated-text) programmatic placeholder token, not natural-language UI */
                <VersionPill
                    label={versionProperties?.version?.versionTag || '<unlabeled>'}
                    isLatest={versionProperties?.isLatest}
                />
                /* eslint-enable i18next/no-literal-string */
            ),
            comment: <Typography.Text ellipsis={{ tooltip: true }}>{versionProperties?.comment}</Typography.Text>,
            createdAt: (
                <StructuredPopover
                    width={250}
                    placement="topRight"
                    sections={[
                        {
                            title: t('createdInSource'),
                            content: dayjs(versionProperties?.createdInSource?.time).format(TIMESTAMP_FORMAT),
                        },
                        {
                            title: t('syncedToDataHub'),
                            content: dayjs(versionProperties?.created?.time).format(TIMESTAMP_FORMAT),
                        },
                    ]}
                >
                    {dayjs(versionProperties?.created?.time).fromNow()}
                </StructuredPopover>
            ),
            menu: (
                <StyledDropdown
                    menu={{ items, style: { borderRadius: '12px', boxShadow: theme.colors.shadowLg } }}
                    trigger={['click']}
                    overlayStyle={{ borderRadius: '100px' }}
                >
                    <MenuIcon icon={DotsThreeVertical} size="2xl" color="gray" />
                </StyledDropdown>
            ),
        };
    });

    return (
        <Drawer
            title={
                <Title size="xl" color="gray" colorLevel={600} weight="semiBold">
                    {t('versionsTitle')}
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
                    placeholder={t('searchPlaceholder')}
                    icon={{ icon: MagnifyingGlass }}
                    value={searchInput}
                    setValue={setSearchInput}
                />
                <Table data={tableData || []} columns={columns} />
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
