import * as React from 'react';
import 'antd/dist/antd.css';
import { Image, Layout, Typography } from 'antd';
import { Link } from 'react-router-dom';
import { SearchBar } from './SearchBar';
import { ManageAccount } from '../shared/ManageAccount';
import { GlobalCfg } from '../../conf';

const { Header } = Layout;

type Props = {
    initialQuery: string;
    placeholderText: string;
    suggestions: Array<string>;
    onSearch: (query: string) => void;
    onQueryChange: (query: string) => void;
    authenticatedUserUrn: string;
    authenticatedUserPictureLink?: string | null;
};

const defaultProps = {
    authenticatedUserPictureLink: undefined,
};

/**
 * A header containing a Logo, Search Bar view, & an account management dropdown.
 */
export const SearchHeader = ({
    initialQuery,
    placeholderText,
    suggestions,
    onSearch,
    onQueryChange,
    authenticatedUserUrn,
    authenticatedUserPictureLink,
}: Props) => {
    return (
        <Header
            style={{
                position: 'fixed',
                zIndex: 1,
                width: '100%',
                backgroundColor: 'rgb(51 62 76)',
                height: '80px',
                lineHeight: '20px',
                color: '#fff',
                padding: '0px 40px',
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
            }}
        >
            <Link
                style={{
                    display: 'flex',
                    alignItems: 'center',
                }}
                to="/"
            >
                <Image style={{ width: '34px', height: '30px' }} src={GlobalCfg.LOGO_IMAGE} preview={false} />
                <Typography.Title level={4} style={{ color: 'white', paddingLeft: '12px', margin: '0' }}>
                    DataHub
                </Typography.Title>
            </Link>
            <SearchBar
                initialQuery={initialQuery}
                placeholderText={placeholderText}
                suggestions={suggestions}
                onSearch={onSearch}
                onQueryChange={onQueryChange}
            />
            <ManageAccount urn={authenticatedUserUrn} pictureLink={authenticatedUserPictureLink || ''} />
        </Header>
    );
};

SearchHeader.defaultProps = defaultProps;
