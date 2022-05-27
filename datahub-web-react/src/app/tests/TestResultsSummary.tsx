import React from 'react';
import { Button, Tag, Typography } from 'antd';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';
import { SUCCESS_COLOR_HEX } from '../entity/shared/tabs/Incident/incidentUtils';
import { useGetSearchResultsForMultipleQuery } from '../../graphql/search.generated';
import { navigateToSearchUrl } from '../search/utils/navigateToSearchUrl';

const StyledButton = styled(Button)`
    margin: 0px;
    padding: 0px;
`;

type Props = {
    urn: string;
};

export const TestResultsSummary = ({ urn }: Props) => {
    // TODO: Replace this

    const history = useHistory();

    const { data: failingResultsData } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                query: '*',
                filters: [
                    {
                        field: 'failingTests',
                        value: `${urn}`,
                    },
                ],
            },
        },
    });

    const { data: passingResultsData } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                query: '*',
                filters: [
                    {
                        field: 'passingTests',
                        value: `${urn}`,
                    },
                ],
            },
        },
    });

    return (
        <>
            <StyledButton
                type="link"
                onClick={() =>
                    navigateToSearchUrl({
                        filters: [
                            {
                                field: 'passingTests',
                                value: urn,
                            },
                        ],
                        history,
                    })
                }
            >
                <Tag>
                    <Typography.Text style={{ color: SUCCESS_COLOR_HEX }} strong>
                        {passingResultsData?.searchAcrossEntities
                            ? passingResultsData?.searchAcrossEntities.total
                            : '-'}{' '}
                    </Typography.Text>
                    passing{' '}
                </Tag>
            </StyledButton>
            <StyledButton
                type="link"
                onClick={() =>
                    navigateToSearchUrl({
                        filters: [
                            {
                                field: 'failingTests',
                                value: urn,
                            },
                        ],
                        history,
                    })
                }
            >
                <Tag>
                    <Typography.Text style={{ color: 'red' }} strong>
                        {failingResultsData?.searchAcrossEntities
                            ? failingResultsData?.searchAcrossEntities.total
                            : '-'}{' '}
                    </Typography.Text>
                    failing
                </Tag>
            </StyledButton>
        </>
    );
};
