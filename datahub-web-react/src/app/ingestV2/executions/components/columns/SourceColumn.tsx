import { colors } from '@components';
import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { ExecutionRequestRecord } from '@app/ingestV2/executions/types';
import { NameColumn } from '@app/ingestV2/source/IngestionSourceTableColumns';

const TextContainer = styled(Typography.Text)`
    color: ${colors.gray[1700]};
`;

interface Props {
    record: ExecutionRequestRecord;
}

export default function SourceColumn({ record }: Props) {
    if (record.type && record.name) {
        return <NameColumn type={record.type} record={record} />;
    }

    return (
        <TextContainer
            ellipsis={{
                tooltip: {
                    title: record.name,
                    color: 'white',
                    overlayInnerStyle: { color: colors.gray[1700] },
                    showArrow: false,
                },
            }}
        >
            Deleted source
        </TextContainer>
    );
}
