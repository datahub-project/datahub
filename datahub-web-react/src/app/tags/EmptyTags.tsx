import React from 'react';
import styled from 'styled-components';
import { Empty, Typography } from 'antd';

type Props = {
    isEmptySearch: boolean;
};

const EmptyContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 40px;
`;

const StyledEmpty = styled(Empty)`
    .ant-empty-description {
        margin-bottom: 12px;
    }
`;

const EmptyTags = ({ isEmptySearch }: Props) => {
    return (
        <EmptyContainer>
            <StyledEmpty
                description={
                    <>
                        <Typography.Text data-testid="tags-not-found">
                            {isEmptySearch ? 'No tags found for your search query' : 'No tags found'}
                        </Typography.Text>
                        <div>
                            {!isEmptySearch && (
                                <Typography.Paragraph>
                                    Tags can be used to organize and categorize data assets across the platform.
                                </Typography.Paragraph>
                            )}
                        </div>
                    </>
                }
                image={Empty.PRESENTED_IMAGE_SIMPLE}
            />
        </EmptyContainer>
    );
};

export default EmptyTags;
