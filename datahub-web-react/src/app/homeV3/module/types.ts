// TODO: adapt to DataHubPageModuleProperties
// the current props are just to draft some components
export interface ModuleProps {
    name: string;
    type: string;
    visibility: 'personal' | 'global';
    description?: string;
    isPublic?: boolean;
}
