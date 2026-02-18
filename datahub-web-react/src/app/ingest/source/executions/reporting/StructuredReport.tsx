import { CloseCircleOutlined, ExclamationCircleOutlined, InfoCircleOutlined } from '@ant-design/icons';
import React from 'react';
import styled, { useTheme } from 'styled-components';

import { StructuredReportItemList } from '@app/ingest/source/executions/reporting/StructuredReportItemList';
import { StructuredReportItemLevel, StructuredReport as StructuredReportType } from '@app/ingest/source/types';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
`;

interface Props {
    report: StructuredReportType;
}

export function StructuredReport({ report }: Props) {
    const theme = useTheme();
    const ERROR_COLOR = theme.colors.iconError;
    const WARNING_COLOR = theme.colors.iconWarning;
    const INFO_COLOR = theme.colors.iconInformation;

    if (!report.items.length) {
        return null;
    }

    const warnings = report.items.filter((item) => item.level === StructuredReportItemLevel.WARN);
    const errors = report.items.filter((item) => item.level === StructuredReportItemLevel.ERROR);
    const infos = report.items.filter((item) => item.level === StructuredReportItemLevel.INFO);
    return (
        <Container>
            {errors.length ? (
                <StructuredReportItemList items={errors} color={ERROR_COLOR} icon={CloseCircleOutlined} />
            ) : null}
            {warnings.length ? (
                <StructuredReportItemList items={warnings} color={WARNING_COLOR} icon={ExclamationCircleOutlined} />
            ) : null}
            {infos.length ? (
                <StructuredReportItemList items={infos} color={INFO_COLOR} icon={InfoCircleOutlined} />
            ) : null}
        </Container>
    );
}
