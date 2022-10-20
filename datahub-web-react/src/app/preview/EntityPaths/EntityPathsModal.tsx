import { Modal } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { EntityPath } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { getDisplayedColumns } from './ColumnPathsText';
import ColumnsRelationshipText from './ColumnsRelationshipText';

const StyledModal = styled(Modal)`
    width: 70vw;
    max-width: 850px;
`;

const Header = styled.div`
    color: ${ANTD_GRAY[8]};
    font-size: 16px;
    padding-top: 8px;
`;

interface Props {
    paths: EntityPath[];
    resultEntityUrn: string;
    hideModal: () => void;
}

export default function EntityPathsModal({ paths, resultEntityUrn, hideModal }: Props) {
    const displayedColumns = getDisplayedColumns(paths, resultEntityUrn);

    return (
        <StyledModal
            title={
                <Header>
                    Column path{paths.length > 1 && 's'} from{' '}
                    <ColumnsRelationshipText displayedColumns={displayedColumns} />
                </Header>
            }
            width="75vw"
            visible
            onCancel={hideModal}
            onOk={hideModal}
            footer={null}
            bodyStyle={{ padding: '16px 24px' }}
        >
            {/* TODO: next PR display CompactEntityNameList with tweaks */}
        </StyledModal>
    );
}
