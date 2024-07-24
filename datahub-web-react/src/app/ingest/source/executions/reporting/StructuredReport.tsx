import React from 'react';
import styled from 'styled-components';
import { CloseCircleOutlined, ExclamationCircleOutlined, InfoCircleOutlined } from '@ant-design/icons';

import { StructuredReportItemLevel, StructuredReport as StructuredReportType } from '../../types';
import { StructuredReportItemList } from './StructuredReportItemList';
import { REDESIGN_COLORS } from '../../../../entity/shared/constants';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 12px;
`;

const ERROR_COLOR = '#F5222D';
const WARNING_COLOR = '#FA8C16';
const INFO_COLOR = REDESIGN_COLORS.BLUE;

interface Props {
    report: StructuredReportType;
}

export function StructuredReport({ report }: Props) {
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
