type AspectsOfSnapshot<Snapshot> = Snapshot extends { aspects: Array<infer Aspect> } ? Aspect : {};

/**
 * Takes a lookup key on the aspects metadata object, and returns an iteratee function that is truthy when it's argument
 * has a metadata object that matches the lookup key
 * @param {SnapshotMetadataAspectKeyName} metadataAspectKey the metadata aspect key to find on the aspect's metadata object
 * @returns {((aspect: ArrayElement<Snapshot['aspects']>) => boolean)}
 */
const getMetadataVersionedAspectAspectKey = <Aspect extends {}, AspectKey extends keyof Aspect>(
  metadataAspectKey: AspectKey
): ((aspect: Aspect) => boolean) => (aspect: Aspect): boolean => aspect.hasOwnProperty(metadataAspectKey);

/**
 * Get the value of the specific metadata keyed by metadataAspectKey from the provided metadata aspect
 * @param {SnapshotMetadataAspectKeyName} metadataAspectKey a string from the enum SnapshotMetadataAspectKeyName, which is the list of keys that may
 * be found on the metadata pair of MetadataAspect instance
 * @param {MetadataAspect} [aspect] optional aspect to read the metadata key from
 * @returns {MetadataAspect['metadata'][SnapshotMetadataAspectKeyName]}
 */
const getMetadataAspectValue = <Aspect extends {}, AspectKey extends keyof Aspect>(
  metadataAspectKey: AspectKey,
  aspect: Aspect
): Aspect[AspectKey] | undefined => (aspect ? aspect[metadataAspectKey] : undefined);

/**
 * Takes a metadata Snapshot instance e.g. IDatasetSnapshot or IMetricSnapshot,
 * and then returns a function that takes a SnapshotMetadataAspectKeyName to lookup the value of that aspect on the snapshot's list of
 * aspects.
 * A snapshot's aspects list can contain multiple metadata aspects, but each can only have one of the keys in SnapshotMetadataAspectKeyName
 * @param {Snapshot} snapshot the metadata snapshot to read from
 * @returns {(metadataAspectKey:SnapshotMetadataAspectKeyName) => MetadataAspect[SnapshotMetadataAspectKeyName]}
 */
export const getMetadataAspect = <
  Snapshot extends { aspects: Array<AspectsOfSnapshot<Snapshot>> },
  AspectKey extends keyof AspectsOfSnapshot<Snapshot>
>(
  snapshot?: Snapshot
): ((key: AspectKey) => AspectsOfSnapshot<Snapshot>[AspectKey] | undefined) => (
  metadataAspectKey: AspectKey
): AspectsOfSnapshot<Snapshot>[AspectKey] | undefined => {
  const { aspects = [] } = snapshot || {};
  // Find the aspect with the metadata key that matches the passed in metadataAspectKey
  const [relevantAspect] = aspects.filter(getMetadataVersionedAspectAspectKey(metadataAspectKey));

  return relevantAspect ? getMetadataAspectValue(metadataAspectKey, relevantAspect) : undefined;
};
