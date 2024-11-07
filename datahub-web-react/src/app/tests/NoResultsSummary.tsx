import React from 'react';
import styled from 'styled-components';
import { Tag, Typography } from 'antd';
import { Popover } from '@components';
import { InfoCircleOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../entity/shared/constants';
import { METADATA_TESTS_DOC_URL } from './constants';

const Container = styled.span`
    max-width: 50px;
`;

const Paragraph = styled.div`
    margin-top: 8px;
    color: ${ANTD_GRAY[7]};
`;

const Section = styled.div`
    margin-bottom: 8px;
`;

const StyledTag = styled(Tag)`
    font-size: 12px;
    color: ${ANTD_GRAY[8]};
    padding-right: 8px;
    padding-left: 8px;
`;

const StyledInfo = styled(InfoCircleOutlined)`
    margin-left: 4px;
    font-size: 12px;
`;

export const NoResultsSummary = () => {
    return (
        <Popover
            overlayStyle={{ maxWidth: 320 }}
            placement="right"
            content={
                <Container>
                    <Typography.Title level={5}>No results found for this Test</Typography.Title>
                    <Paragraph>
                        <Section>
                            Either 0 data assets were selected by this Test, or the test has not been evaluated for any
                            selected data assets yet.
                        </Section>
                        <Section>
                            Each Test is evaluated once per day. It can take up to 24 hours to be evaluated for the
                            first time.
                        </Section>
                        <Section>
                            Learn more about Metadata Tests{' '}
                            <a href={METADATA_TESTS_DOC_URL} target="_blank" rel="noopener noreferrer">
                                here.
                            </a>
                        </Section>
                        <Section>
                            Still have questions or feedback? Reach out to your Acryl customer representative!
                        </Section>
                    </Paragraph>
                </Container>
            }
        >
            <StyledTag color={ANTD_GRAY[3]}>
                No results found
                <StyledInfo />
            </StyledTag>
        </Popover>
    );
};
