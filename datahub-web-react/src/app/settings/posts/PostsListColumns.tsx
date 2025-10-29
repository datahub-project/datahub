import { Maybe } from 'graphql/jsutils/Maybe';
import styled from 'styled-components/macro';

export interface PostEntry {
    urn: string;
    title: string;
    contentType: string;
    description?: Maybe<string>;
    link?: string | null;
    imageUrl?: string;
}
