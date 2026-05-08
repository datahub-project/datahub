import { Typography } from 'antd';
import React from 'react';
import styled, { useTheme } from 'styled-components';

import { ExecutionRequestRecord } from '@app/ingestV2/executions/types';
import { NameColumn } from '@app/ingestV2/source/IngestionSourceTableColumns';

const TextContainer = styled(Typography.Text)`
    color: ${(props) => props.theme.colors.textSecondary};
`;

interface Props {
    record: ExecutionRequestRecord;
    navigateToSource: () => void;
}

export default function SourceColumn({ record, navigateToSource }: Props) {
    const theme = useTheme();

    if (record.type && record.name) {
        return <NameColumn type={record.type} record={record} onNameClick={navigateToSource} />;
    }

    return (
        <TextContainer
            ellipsis={{
                tooltip: {
                    title: record.name,
                    overlayInnerStyle: { color: theme.colors.textSecondary },
                    showArrow: false,
                },
            }}
        >
            Deleted source
        </TextContainer>
    );
}
