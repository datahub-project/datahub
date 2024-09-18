import { Icon, Pill, Table, Text } from '@components';
import { AlignmentOptions } from '@src/alchemy-components/theme/config';
import { toRelativeTimeString } from '@src/app/shared/time/timeUtils';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { GetSearchResultsForMultipleQuery } from '@src/graphql/search.generated';
import TableIcon from '@src/images/table-icon.svg?react';
import { SearchResult } from '@src/types.generated';
import { Dropdown, Tooltip } from 'antd';
import React from 'react';
import { CardIcons } from '../Dashboard/Forms/styledComponents';
import {
    DataContainer,
    IconContainer,
    MenuItem,
    NameColumn,
    PillContainer,
    PillsContainer,
    PropDescription,
    PropName,
} from './styledComponents';
import { getDisplayName } from './utils';

interface Props {
    searchQuery: string;
    data: GetSearchResultsForMultipleQuery | undefined;
    loading: boolean;
    setIsDrawerOpen: React.Dispatch<React.SetStateAction<boolean>>;
    setCurrentProperty: React.Dispatch<React.SetStateAction<SearchResult | undefined>>;
}

const StructuredPropsTable = ({ searchQuery, data, loading, setIsDrawerOpen, setCurrentProperty }: Props) => {
    const entityRegistry = useEntityRegistryV2();

    const structuredProperties = data?.searchAcrossEntities?.searchResults || [];

    // Filter the table data based on the search query
    const filteredProperties = structuredProperties.filter((prop: any) =>
        prop.entity.definition.displayName?.toLowerCase().includes(searchQuery.toLowerCase()),
    );

    const columns = [
        {
            title: 'Name',
            key: 'name',
            render: (record) => {
                return (
                    <NameColumn>
                        <IconContainer>
                            <TableIcon />
                        </IconContainer>
                        <DataContainer>
                            <PropName>{getDisplayName(record.entity)}</PropName>
                            <PropDescription>{record.entity.definition.description}</PropDescription>
                        </DataContainer>
                    </NameColumn>
                );
            },
            width: '580px',
            sorter: (sourceA, sourceB) => {
                return getDisplayName(sourceA.entity).localeCompare(getDisplayName(sourceB.entity));
            },
        },
        {
            title: 'Entity Types',
            key: 'entityTypes',
            width: '270px',
            render: (record) => {
                const types = record.entity.definition.entityTypes;
                const maxTypesToShow = 2;
                const overflowCount = types.length - maxTypesToShow;

                return (
                    <PillsContainer>
                        {types.slice(0, maxTypesToShow).map((entityType) => {
                            const typeName = entityRegistry.getEntityName(entityType.info.type);
                            return (
                                <PillContainer>{typeName && <Pill label={typeName} clickable={false} />}</PillContainer>
                            );
                        })}
                        {overflowCount > 0 && (
                            <Tooltip
                                title={types
                                    .slice(maxTypesToShow)
                                    .map((eType) => {
                                        const name = entityRegistry.getEntityName(eType.info.type);
                                        return name;
                                    })
                                    .join(', ')}
                            >
                                <>
                                    <Text>{`+${overflowCount}`}</Text>
                                </>
                            </Tooltip>
                        )}
                    </PillsContainer>
                );
            },
        },
        {
            title: 'Creation Date',
            key: 'creationDate',
            render: (record) => {
                const createdTime = record.entity.definition.creationDate;
                return createdTime ? toRelativeTimeString(createdTime) : '-';
            },
        },

        {
            title: 'Created By',
            key: 'createdBy',
            render: () => {},
        },
        {
            title: '',
            key: 'actions',
            alignment: 'right' as AlignmentOptions,
            render: (record) => {
                const items = [
                    {
                        key: '0',
                        label: (
                            <MenuItem
                                onClick={() => {
                                    setIsDrawerOpen(true);
                                    setCurrentProperty(record);
                                }}
                            >
                                Edit
                            </MenuItem>
                        ),
                    },
                    {
                        key: '1',
                        label: <MenuItem>Delete</MenuItem>,
                    },
                ];
                return (
                    <>
                        <CardIcons>
                            <Dropdown menu={{ items }} trigger={['click']}>
                                <Icon icon="MoreVert" size="md" />
                            </Dropdown>
                        </CardIcons>
                    </>
                );
            },
        },
    ];
    return <Table columns={columns} data={filteredProperties} isLoading={loading} isScrollable />;
};

export default StructuredPropsTable;
