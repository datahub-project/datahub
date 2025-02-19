import React from 'react';
import styled from 'styled-components';
import { PlusOutlined } from '@ant-design/icons';
import { ANTD_GRAY, ANTD_GRAY_V2, REDESIGN_COLORS } from '../../../entity/shared/constants';

const DomainInfoContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

const DataProductDescription = styled.div`
    font-size: 14px;
    font-weight: 400;
    color: ${ANTD_GRAY[7]};
    font-family: Mulish;
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
    max-width: 200px;
`;

const DataProductTitle = styled.div`
    font-size: 16px;
    font-weight: 400;
    color: ${REDESIGN_COLORS.BLUE};
    font-family: Mulish;
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
    max-width: 200px;
`;

const Card = styled.div`
    align-self: stretch;
    align-items: center;
    background-color: ${ANTD_GRAY[1]};
    border: 1.5px solid ${ANTD_GRAY_V2[5]};
    border-radius: 10px;
    display: flex;
    justify-content: start;
    min-width: 160px;
    padding: 16px;
    height: 100%;
    :hover {
        border: 1.5px solid ${REDESIGN_COLORS.BLUE};
        cursor: pointer;
    }
`;

export const DataProductMiniPreviewAddDataProduct = ({ onAdd }: { onAdd: () => void }): JSX.Element => {
    return (
        <Card onClick={onAdd}>
            <DomainInfoContainer>
                <DataProductTitle>
                    <PlusOutlined style={{ marginRight: 4 }} />
                    Add Data Product
                </DataProductTitle>
                <DataProductDescription>Share your knowledge</DataProductDescription>
            </DomainInfoContainer>
        </Card>
    );
};
