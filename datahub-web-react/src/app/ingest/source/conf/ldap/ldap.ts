import { SourceConfig } from '../types';
import ldapLogo from '../../../../../images/ldaplogo.png';

const placeholderRecipe = `\
source:
    type: ldap
    config:
        ldap_server: 
        ldap_user:
        ldap_password: 
        base_dn: 
sink:
    type: datahub-rest
    config:
        server: 'http://datahub-gms:8080'
`;

const ldapConfig: SourceConfig = {
	type: 'ldap',
	placeholderRecipe,
	displayName: 'LDAP',
	docsUrl: 'https://datahubproject.io/docs/generated/ingestion/sources/ldap',
	logoUrl: ldapLogo,
};

export default ldapConfig;
