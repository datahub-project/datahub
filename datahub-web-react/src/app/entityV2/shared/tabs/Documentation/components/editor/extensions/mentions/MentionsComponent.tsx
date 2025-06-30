import { LoadingOutlined } from '@ant-design/icons';
import { FloatingWrapper } from '@remirror/react';
import { Empty, Spin } from 'antd';
import React, { useEffect, useState } from 'react';
import { useDebounce } from 'react-use';
import { Positioner, selectionPositioner } from 'remirror/extensions';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { MentionsDropdown } from '@app/entityV2/shared/tabs/Documentation/components/editor/extensions/mentions/MentionsDropdown';
import { useDataHubMentions } from '@app/entityV2/shared/tabs/Documentation/components/editor/extensions/mentions/useDataHubMentions';

import { useGetAutoCompleteMultipleResultsLazyQuery } from '@graphql/search.generated';

const Container = styled.div`
    position: relative;
    top: 0;
    left: 0;
    .ant-spin-container {
        overflow: auto;
        max-height: 350px;
    }
`;

const StyledEmpty = styled(Empty)`
    margin: 16px;
`;

export const MentionsComponent = () => {
    const userContext = useUserContext();
    const [getAutoComplete, { data: autocompleteData, loading }] = useGetAutoCompleteMultipleResultsLazyQuery();
    const { active, range, filter: query } = useDataHubMentions({});
    const [suggestions, setSuggestions] = useState<any[]>([]);
    const viewUrn = userContext.localState?.selectedViewUrn;

    useEffect(() => {
        if (query) {
            getAutoComplete({ variables: { input: { query, viewUrn } } });
        }
    }, [getAutoComplete, query, viewUrn]);
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
                <Spin spinning={loading} delay={100} indicator={<LoadingOutlined />}>
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
