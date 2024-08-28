import React from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';
import { PageRoutes } from '../../../../conf/Global';
import { REDESIGN_COLORS } from '../../../entityV2/shared/constants';
import { StyledButton } from '../../../shared/share/v2/styledComponents';
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

    return (
        <Container>
            <FormsSection>
                <SectionHeader>
                    <HeaderText>All Forms</HeaderText>
                    <StyledButton
                        $color={REDESIGN_COLORS.TITLE_PURPLE}
                        $type="filled"
                        onClick={() => history.push(PageRoutes.NEW_FORM)}
                    >
                        Create Form
                    </StyledButton>
                </SectionHeader>
                <FormsContainer>
                    <FormsTable />
                </FormsContainer>
            </FormsSection>
        </Container>
    );
};

export default FormsTab;
