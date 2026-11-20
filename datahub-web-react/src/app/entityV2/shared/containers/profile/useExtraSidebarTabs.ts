import { EntitySidebarTab, TabContextType } from '@app/entityV2/shared/types';

/**
 * Extra sidebar tabs contributed on top of an entity's registered tabs.
 * OSS contributes none; the hook exists so that `useFinalSidebarTabs` is the
 * single place where the sidebar's tab list is assembled.
 */
export function useExtraSidebarTabs(baseTabs: EntitySidebarTab[], _contextType: TabContextType): EntitySidebarTab[] {
    return baseTabs;
}
