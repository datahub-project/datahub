import React from 'react';
import styled from 'styled-components';
import { Button, Empty, Typography } from 'antd';
import { PlusOutlined } from '@ant-design/icons';
import { useHistory } from 'react-router';

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
    const history = useHistory();

    return (
        <EmptyContainer>
            <StyledEmpty
                description={
                    <>
                        <Typography.Text>
                            {isEmptySearch ? 'No tags found for your search query' : 'No tags found'}
                        </Typography.Text>
                        <div>
                            {!isEmptySearch && (
                                <Typography.Paragraph>
                                    Tags can be used to organize and categorize data assets across the platform.
                                </Typography.Paragraph>
                            )}
                        </div>
                        <Button
                            type="primary"
                            onClick={() => history.push('/tag/new')}
                            data-testid="add-tag-button"
                            icon={<PlusOutlined />}
                        >
                            Create New Tag
                        </Button>
                    </>
                }
                image={Empty.PRESENTED_IMAGE_SIMPLE}
            />
        </EmptyContainer>
    );
};

export default EmptyTags;
