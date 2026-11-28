import { Button, Pill, Table, Text, Tooltip } from '@components';
import { Copy } from '@phosphor-icons/react/dist/csr/Copy';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import type { Column } from '@components/components/Table/types';

import { getCategoryPillColors, getResultPillProps } from '@app/entityV2/shared/tabs/Dataset/Governance/testUtils';

import { TestResult, TestResultType } from '@types';

const TableContainer = styled.div`
    flex: 1;
    min-height: 0;
    padding: 0 20px 20px;
`;

// Table cells default to a single clipped line; names and descriptions are prose, so let them wrap.
const PrimaryText = styled.div`
    color: ${(props) => props.theme.colors.text};
    white-space: normal;
`;

const WrappingText = styled.div`
    color: ${(props) => props.theme.colors.textSecondary};
    white-space: normal;
`;

type ResultRow = {
    urn: string;
    name: string;
    category: string;
    description?: string | null;
    resultType: TestResultType;
};

type Props = {
    results: Array<TestResult>;
};

function toRows(results: Array<TestResult>): ResultRow[] {
    return results.flatMap((result) =>
        result.test
            ? [
                  {
                      urn: result.test.urn,
                      name: result.test.name,
                      category: result.test.category,
                      description: result.test.description,
                      resultType: result.type,
                  },
              ]
            : [],
    );
}

export const TestResultsList = ({ results }: Props) => {
    const { t } = useTranslation(['entity.profile.tests', 'entity.profile.queries', 'shared.misc']);
    const rows = toRows(results);
    const categoryColors = getCategoryPillColors(rows.map((row) => row.category));

    const columns: Column<ResultRow>[] = [
        {
            title: t('entity.profile.tests:table.test'),
            key: 'name',
            width: '240px',
            render: (row) => (
                <PrimaryText>
                    <Text weight="semiBold">{row.name}</Text>
                </PrimaryText>
            ),
            sorter: (a, b) => a.name.localeCompare(b.name),
        },
        {
            title: t('entity.profile.tests:table.description'),
            key: 'description',
            render: (row) => (
                <WrappingText>
                    <Text>{row.description || t('entity.profile.queries:queryCard.noDescription')}</Text>
                </WrappingText>
            ),
        },
        {
            title: t('entity.profile.tests:table.category'),
            key: 'category',
            width: '160px',
            render: (row) => (
                <Pill
                    label={row.category}
                    variant="squareFilled"
                    color={categoryColors.get(row.category)}
                    size="sm"
                    clickable={false}
                />
            ),
            sorter: (a, b) => a.category.localeCompare(b.category),
        },
        {
            title: t('entity.profile.tests:table.status'),
            key: 'status',
            width: '120px',
            render: (row) => <Pill {...getResultPillProps(row.resultType)} size="sm" clickable={false} />,
            // Failing sorts ahead of passing in ascending order.
            sorter: (a, b) =>
                Number(a.resultType === TestResultType.Success) - Number(b.resultType === TestResultType.Success),
        },
        {
            title: '',
            key: 'actions',
            width: '56px',
            alignment: 'right',
            render: (row) =>
                navigator.clipboard && (
                    <Tooltip title={t('shared.misc:copyUrn.tooltip')}>
                        <Button
                            variant="text"
                            color="gray"
                            size="sm"
                            isCircle
                            icon={{ icon: Copy }}
                            aria-label={t('shared.misc:copyUrn.tooltip')}
                            onClick={() => navigator.clipboard.writeText(row.urn)}
                        />
                    </Tooltip>
                ),
        },
    ];

    return (
        <TableContainer>
            <Table<ResultRow> columns={columns} data={rows} rowKey="urn" isScrollable maxHeight="100%" />
        </TableContainer>
    );
};
