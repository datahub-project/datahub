import buildEntityRegistryV2 from '@app/buildEntityRegistryV2';
import EntityRegistry from '@app/entityV2/EntityRegistry';

let registry: EntityRegistry | undefined;

// A Proxy is used so that the default export is a live reference to registry,
// rather than a snapshot of undefined captured at module load time.
// The registry is built on first access (lazy initialization).
export default new Proxy({} as EntityRegistry, {
    get(_, prop) {
        if (!registry) {
            registry = buildEntityRegistryV2();
        }
        return Reflect.get(registry, prop);
    },
});
