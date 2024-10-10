import { Button } from '@components';
import analytics, { EventType } from '@src/app/analytics';
import { useUserContext } from '@src/app/context/useUserContext';
import { Tooltip } from 'antd';
import React, { useState } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';
import { PageRoutes } from '../../../../conf/Global';
import { REDESIGN_COLORS } from '../../../entityV2/shared/constants';
import { StyledSearch } from '../../structuredProperties/styledComponents';
import FormsTable from './FormsTable';

const Container = styled.div`
    display: flex;
    margin: 20px;
    overflow: auto;
    height: calc(100% - 40px);
`;

const SectionHeader = styled.div`
    display: flex;
    justify-content: space-between;
`;
const HeaderText = styled.div`
    display: flex;
    color: ${REDESIGN_COLORS.TEXT_HEADING_SUB_LINK};
    font-size: 18px;
    font-weight: 700;
`;

const FormsSection = styled.div`
    display: flex;
    flex-direction: column;
    width: 100%;
    gap: 20px;
`;

const FormsContainer = styled.div`
    display: flex;
    overflow: auto;
    flex: 1;
`;

const FormsTab = () => {
    const history = useHistory();
    const me = useUserContext();
    const canEditForms = me.platformPrivileges?.manageDocumentationForms;

    const [searchQuery, setSearchQuery] = useState<string>('');

    const handleSearch = (value) => {
        setSearchQuery(value);
    };

    return (
        <Container>
            <FormsSection>
                <SectionHeader>
                    <HeaderText>Your Forms</HeaderText>
                    <Tooltip
                        showArrow={false}
                        title={
                            !canEditForms
                                ? 'Must have permission to manage forms. Ask your DataHub administrator.'
                                : null
                        }
                    >
                        <>
                            <Button
                                icon="Add"
                                onClick={() => {
                                    analytics.event({
                                        type: EventType.CreateFormClickEvent,
                                    });
                                    history.push(PageRoutes.NEW_FORM);
                                }}
                                disabled={!canEditForms}
                            >
                                Create
                            </Button>
                        </>
                    </Tooltip>
                </SectionHeader>
                <StyledSearch
                    placeholder="Search"
                    onSearch={handleSearch}
                    onChange={(e) => handleSearch(e.target.value)}
                    allowClear
                />
                <FormsContainer>
                    <FormsTable searchQuery={searchQuery} />
                </FormsContainer>
            </FormsSection>
        </Container>
    );
};

export default FormsTab;
