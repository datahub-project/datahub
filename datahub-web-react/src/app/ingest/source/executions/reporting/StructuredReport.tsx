import React from 'react';
import styled from 'styled-components';
import { CloseCircleOutlined, ExclamationCircleOutlined } from '@ant-design/icons';

import { StructuredReport as StructuredReportType } from '../../types';
import { StructuredReportItemList } from './StructuredReportItemList';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
`;

const FAILURE_COLOR = '#F5222D';
const WARNING_COLOR = '#FA8C16';

interface Props {
    report: StructuredReportType;
}

export function StructuredReport({ report }: Props) {
    const { failures, warnings } = report.source.report;

    if (!failures.length && !warnings.length) {
        return null;
    }

    return (
        <Container>
            {failures.length ? (
                <StructuredReportItemList items={failures} color={FAILURE_COLOR} icon={CloseCircleOutlined} />
            ) : null}
            {warnings.length ? (
                <StructuredReportItemList items={warnings} color={WARNING_COLOR} icon={ExclamationCircleOutlined} />
            ) : null}
        </Container>
    );
}
