import { PageTemplateSurfaceType } from '@types';

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export default function useIsTemplateEditable(templateType: PageTemplateSurfaceType) {
    // Editing is disabled in OSS
    return false;
}
