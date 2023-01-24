import React, { useEffect, useState } from 'react';
import { useDebounce } from 'react-use';
import { FloatingWrapper } from '@remirror/react';
import { Empty, Spin } from 'antd';
import styled from 'styled-components';
import { Positioner, selectionPositioner } from 'remirror/extensions';
import { useGetAutoCompleteMultipleResultsLazyQuery } from '../../../../../../../../../graphql/search.generated';
import { MentionsDropdown } from './MentionsDropdown';
import { useDataHubMentions } from './useDataHubMentions';

const Container = styled.div`
    position: relative;
    top: 0;
    left: 0;
`;

const StyledEmpty = styled(Empty)`
    margin: 16px;
`;

export const MentionsComponent = () => {
    const [getAutoComplete, { data: autocompleteData, loading }] = useGetAutoCompleteMultipleResultsLazyQuery();
    const { active, range, filter: query } = useDataHubMentions({});
    const [suggestions, setSuggestions] = useState<any[]>([]);

    useEffect(() => {
        if (query) {
            getAutoComplete({ variables: { input: { query } } });
        }
    }, [getAutoComplete, query]);
    useDebounce(() => setSuggestions(autocompleteData?.autoCompleteForMultiple?.suggestions || []), 250, [
        autocompleteData,
    ]);

    if (!active) return null;
    const mentionsPositioner = selectionPositioner.clone(() => ({
        getActive: ({ view }) => {
            try {
                if (!range) return Positioner.EMPTY;
                return [{ from: view.coordsAtPos(range.from), to: view.coordsAtPos(range.to) }];
            } catch {
                return Positioner.EMPTY;
            }
        },
    }));

    return (
        <FloatingWrapper positioner={mentionsPositioner} enabled={active} placement="bottom-start">
            <Container className="ant-select-dropdown">
                <Spin spinning={loading} delay={100}>
                    {suggestions?.length > 0 ? (
                        <MentionsDropdown suggestions={suggestions} />
                    ) : (
                        <StyledEmpty image={Empty.PRESENTED_IMAGE_SIMPLE} description="No results found" />
                    )}
                </Spin>
            </Container>
        </FloatingWrapper>
    );
};
