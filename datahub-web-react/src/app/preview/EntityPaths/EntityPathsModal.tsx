import { Modal } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';
import { Entity, EntityPath } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { CompactEntityNameList } from '../../recommendations/renderer/component/CompactEntityNameList';
import { getDisplayedColumns } from './ColumnPathsText';
import ColumnsRelationshipText from './ColumnsRelationshipText';

const StyledModal = styled(Modal)`
    width: 70vw;
    max-width: 850px;
`;

const PathWrapper = styled.div`
    display: inline-block;
    margin: 15px 0 15px -4px;
    padding: 20px;
    border: 1px solid ${ANTD_GRAY[4]};
    border-radius: 8px;
    box-shadow: 1px 1px 12px 4px #0000000d;
    width: 100%;
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
            {paths.map((path) => (
                <PathWrapper>
                    <CompactEntityNameList entities={path.path as Entity[]} showArrows />
                </PathWrapper>
            ))}{' '}
        </StyledModal>
    );
}
