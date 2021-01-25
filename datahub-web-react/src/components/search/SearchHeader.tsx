import * as React from 'react';
import 'antd/dist/antd.css';
import { Image, Layout } from 'antd';
import { Link } from 'react-router-dom';
import { SearchBar } from './SearchBar';
import { ManageAccount } from '../shared/ManageAccount';
import { GlobalCfg } from '../../conf';

const { Header } = Layout;

type Props = {
    types: Array<string>;
    initialType: string;
    initialQuery: string;
    placeholderText: string;
    suggestions: Array<string>;
    onSearch: (type: string, query: string) => void;
    onQueryChange: (type: string, query: string) => void;
    authenticatedUserUrn: string;
    authenticatedUserPictureLink?: string;
};

const defaultProps = {
    authenticatedUserPictureLink: undefined,
};

/**
 * A header containing a Logo, Search Bar view, & an account management dropdown.
 */
export const SearchHeader = ({
    types: _types,
    initialType: _initialType,
    initialQuery: _initialQuery,
    placeholderText: _placeholderText,
    suggestions: _suggestions,
    onSearch: _onSearch,
    onQueryChange: _onQueryChange,
    authenticatedUserUrn: _authenticatedUserUrn,
    authenticatedUserPictureLink: _authenticatedUserPictureLink,
}: Props) => {
    return (
        <Header
            style={{
                position: 'fixed',
                zIndex: 1,
                width: '100%',
                backgroundColor: 'rgb(51 62 76)',
                fontSize: '18px',
                height: '64px',
                lineHeight: '20px',
                color: '#fff',
                padding: '0px 80px',
            }}
        >
            <div style={{ display: 'flex', alignItems: 'center' }}>
                <Link
                    style={{
                        height: '64px',
                        padding: '15px 30px',
                        display: 'flex',
                        alignItems: 'center',
                    }}
                    to="/"
                >
                    <Image style={{ width: '34px', height: '30px' }} src={GlobalCfg.LOGO_IMAGE} preview={false} />
                    <div style={{ color: 'white', fontWeight: 'bold', padding: '15px' }}>DataHub</div>
                </Link>
                <SearchBar
                    types={_types}
                    initialQuery={_initialQuery}
                    initialType={_initialType}
                    placeholderText={_placeholderText}
                    suggestions={_suggestions}
                    onSearch={_onSearch}
                    onQueryChange={_onQueryChange}
                />
                <ManageAccount urn={_authenticatedUserUrn} pictureLink={_authenticatedUserPictureLink} />
            </div>
        </Header>
    );
};

SearchHeader.defaultProps = defaultProps;
