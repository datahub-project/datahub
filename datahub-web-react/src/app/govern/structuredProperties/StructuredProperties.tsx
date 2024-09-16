import { Button, Text } from '@src/alchemy-components';
import React, { useState } from 'react';
import StructuredPropsTable from './StructuredPropsTable';
import {
    ButtonContainer,
    HeaderContainer,
    HeaderContent,
    PageContainer,
    StyledSearch,
    TableContainer,
} from './styledComponents';

const StructuredProperties = () => {
    const [searchQuery, setSearchQuery] = useState<string>('');

    const handleSearch = (value) => {
        setSearchQuery(value);
    };

    return (
        <PageContainer>
            <HeaderContainer>
                <HeaderContent>
                    <Text size="xl" weight="bold">
                        Structured Properties
                    </Text>
                    <Text color="gray" weight="medium">
                        Information about this page
                    </Text>
                </HeaderContent>
                <ButtonContainer>
                    <Button icon="Add">Create</Button>
                </ButtonContainer>
            </HeaderContainer>
            <StyledSearch
                placeholder="Search"
                onSearch={handleSearch}
                onChange={(e) => handleSearch(e.target.value)}
                allowClear
            />

            <TableContainer>
                <StructuredPropsTable searchQuery={searchQuery} />
            </TableContainer>
        </PageContainer>
    );
};

export default StructuredProperties;
