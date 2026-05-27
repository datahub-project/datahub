import { LoadingOutlined } from '@ant-design/icons';
import { Spin } from 'antd';
import React from 'react';

import { HelperText, ResultContainer } from '@app/context/import/components/importDocumentsModal.styles';
import { Text } from '@src/alchemy-components';

export default function ImportDocumentsImportingStep() {
    return (
        <ResultContainer>
            <Spin indicator={<LoadingOutlined style={{ fontSize: 36 }} spin />} />
            <Text weight="semiBold">Importing documents...</Text>
            <HelperText>This may take a moment</HelperText>
        </ResultContainer>
    );
}
