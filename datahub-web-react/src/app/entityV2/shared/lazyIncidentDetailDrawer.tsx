import { lazyProfileComponent } from '@app/entityV2/shared/lazyEntityProfile';

// Kept out of profileChunks.tsx so menu actions can lazy-load the drawer without a
// chunk-boundary cycle (profileChunks → EntityProfile → … → RaiseIncidentMenuAction).

export const IncidentDetailDrawer = lazyProfileComponent('IncidentDetailDrawer', () =>
    import('@app/entityV2/shared/tabs/Incident/AcrylComponents/IncidentDetailDrawer').then((module) => ({
        default: module.IncidentDetailDrawer,
    })),
);
