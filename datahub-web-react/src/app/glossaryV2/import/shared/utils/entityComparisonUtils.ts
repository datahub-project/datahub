/**
 * Entity Comparison Utilities
 * Shared functions for comparing entity data
 */
import { Entity } from '@app/glossaryV2/import/glossary.types';

/**
 * Check if entity info (name, description, term_source, source_ref, source_url, parentNames) has changed
 */
export function hasEntityInfoChanged(newEntity: Entity, existingEntity: Entity): boolean {
    if (newEntity.name !== existingEntity.name) return true;

    const newDescription = newEntity.data.description || '';
    const existingDescription = existingEntity.data.description || '';
    if (newDescription !== existingDescription) return true;

    const newTermSource = newEntity.data.term_source || '';
    const existingTermSource = existingEntity.data.term_source || '';
    if (newTermSource !== existingTermSource) return true;

    const newSourceRef = newEntity.data.source_ref || '';
    const existingSourceRef = existingEntity.data.source_ref || '';
    if (newSourceRef !== existingSourceRef) return true;

    const newSourceUrl = newEntity.data.source_url || '';
    const existingSourceUrl = existingEntity.data.source_url || '';
    if (newSourceUrl !== existingSourceUrl) return true;

    const newParentNames = newEntity.parentNames || [];
    const existingParentNames = existingEntity.parentNames || [];
    if (JSON.stringify(newParentNames) !== JSON.stringify(existingParentNames)) return true;

    return false;
}
