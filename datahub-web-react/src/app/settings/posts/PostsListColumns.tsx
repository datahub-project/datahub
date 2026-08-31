import { Maybe } from 'graphql/jsutils/Maybe';

export interface PostEntry {
    urn: string;
    title: string;
    contentType: string;
    description?: Maybe<string>;
    link?: string | null;
    imageUrl?: string;
}
