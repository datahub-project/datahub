/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Empty, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

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
                                    Tags can be used to organize data assets in DataHub.
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
