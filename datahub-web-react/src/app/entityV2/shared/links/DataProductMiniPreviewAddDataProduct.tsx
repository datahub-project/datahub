import { PlusOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';


const DomainInfoContainer = styled.div`
    display: flex;
    flex-direction: column;
`;

const DataProductDescription = styled.div`
    font-size: 14px;
    font-weight: 400;
    color: ${(props) => props.theme.colors.textTertiary};
    font-family: Mulish;
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
    max-width: 200px;
`;

const DataProductTitle = styled.div`
    font-size: 16px;
    font-weight: 400;
    color: ${(props) => props.theme.colors.textInformation};
    font-family: Mulish;
    overflow: hidden;
    white-space: nowrap;
    text-overflow: ellipsis;
    max-width: 200px;
`;

const Card = styled.div`
    align-self: stretch;
    align-items: center;
    background-color: ${(props) => props.theme.colors.bg};
    border: 1.5px solid ${(props) => props.theme.colors.border};
    border-radius: 10px;
    display: flex;
    justify-content: start;
    min-width: 160px;
    padding: 16px;
    height: 100%;
    :hover {
        border: 1.5px solid ${(props) => props.theme.colors.textInformation};
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
