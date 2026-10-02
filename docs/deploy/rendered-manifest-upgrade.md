
# Upgrading with rendered manifests

A zero-downtime upgrade (ZDU) has three phases:

1. A **blocking** system-update Job (`-u SystemUpdateBlocking`)
2. A rollout of DataHub components
3. A **non-blocking** system-update Job (`-u SystemUpdateNonBlocking`)

`helm upgrade` orders those phases with Helm hooks. Docker Compose orders the blocking Job with `depends_on`. Argo CD can map the same hooks to a PreSync wave and a PostSync wave.

This page is for pipelines that render the [DataHub Helm chart](https://github.com/acryldata/datahub-helm) and apply the YAML themselves, with Spinnaker, Kustomize, or `kubectl`. GMS, MAE, and MCE do not wait for system-update to finish. Applying the whole render in one step starts the new pods and both Jobs together.

Follow this procedure only when ZDU is enabled (`global.datahub.systemUpdate.zdu.enable: true`, the default on current charts).

What the two Jobs do is described in the [datahub-upgrade image](../../docker/datahub-upgrade/README.md). How blocking and background steps are split is described in [Bootstrap MCPs](../advanced/bootstrap-mcps.md).

## Render from the chart on every deploy

Keep a pinned chart version and your values files in version control. That pair is the source of truth. Every deploy — upgrade, config change, or rerun — renders again so chart logic is applied each time: ZDU environment variables, scale-down suppression, Job arguments chosen by `datahubSystemUpdate.nonblocking.enabled`, SQL and Elasticsearch setup environment, secret wiring, and behavior that ships in a newer chart.

```bash
helm repo add datahub https://helm.datahubproject.io/
helm repo update

helm template <release> datahub/datahub \
  --version <chart-version> \
  -n <namespace> \
  -f values.yaml \
  > build/datahub.yaml
```

To upgrade DataHub, bump the chart version, `global.datahub.version`, or both, then render again.

:::caution Do not maintain rendered YAML

Generate the render on every deploy. You may save it to review a diff. It is a build artifact.

Do not commit rendered manifests as the files you edit. Do not hand-edit image tags or environment variables in rendered output, and do not copy Job specs forward between releases. Rendered YAML drifts from the chart. A later chart adds or renames environment variables and upgrade steps. A hand-maintained manifest misses those changes and can run the wrong upgrade mode, for example by dropping `ZDU_STAGE_20` or the `SystemUpdateBlocking` arguments.

:::

Put customizations in chart values first: `extraEnvs`, annotations, labels, resources, and `image.args`. Anything values cannot express goes in a Kustomize patch or a Spinnaker manifest override, applied to a fresh render every time and targeted by kind and name.

SQL, Kafka, and Elasticsearch or OpenSearch must already be running. They are outside these three phases.

## Values

Set these explicitly, or leave the chart defaults:

```yaml
global:
  datahub:
    systemUpdate:
      enabled: true
      consolidatedUpgrade: true
      zdu:
        preEnable: true
        enable: true
datahubSystemUpdate:
  nonblocking:
    enabled: true
```

`zdu.preEnable: true` sets `ZDU_STAGE_10` on the system-update Jobs. `zdu.enable: true` sets `ZDU_STAGE_20` on those Jobs and on GMS, and sets `DATAHUB_UPGRADE_K8_SCALE_DOWN_ENABLED` to `false`. Leave those variables as the chart renders them.

`datahubSystemUpdate.nonblocking.enabled: false` renders one Job with `-u SystemUpdate`. That is a different procedure from the three phases below.

Keep `consolidatedUpgrade: true`. SQL and index setup run inside system-update. The separate Elasticsearch, Kafka, and SQL setup Jobs that `consolidatedUpgrade: false` renders are not supported. Do not turn them on for this procedure.

## Hooks do not order an apply

The chart annotates `<release>-system-update` with `helm.sh/hook: pre-install,pre-upgrade` (weight `-4`) and `<release>-system-update-nonblk` with `helm.sh/hook: post-install,post-upgrade`. Helm reads those annotations during `helm install` and `helm upgrade`. `kubectl apply`, Kustomize, and Spinnaker create every object in the file as soon as they see it. Split the render and apply the three phases yourself. Leave the hook annotations in place. Removing them does not create ordering.

`helm.sh/hook-delete-policy: before-hook-creation` is Helm-only as well. Job specs are immutable, so delete the previous Job before applying a new one.

The chart sets `DATAHUB_REVISION` from the Helm release revision. `helm template` leaves that value at `1`. Leave it as rendered. It is not the ordering gate.

## The three phases

Job names follow the Helm release name. The examples use release `datahub`, namespace `datahub`, and mikefarah [yq](https://github.com/mikefarah/yq) v4. Write the render into a gitignored build directory.

```bash
export RELEASE=datahub
export NAMESPACE=datahub
export CHART_VERSION=<chart-version>
BUILD=build/datahub-render
mkdir -p "$BUILD"

helm template "$RELEASE" datahub/datahub \
  --version "$CHART_VERSION" \
  -n "$NAMESPACE" \
  -f values.yaml \
  > "$BUILD/all.yaml"

# Phase 1a: objects the blocking Job needs. Applied before the Job.
yq 'select(
      .kind == "Secret" or .kind == "ConfigMap"
      or .kind == "ServiceAccount" or .kind == "Role" or .kind == "RoleBinding"
    )' "$BUILD/all.yaml" > "$BUILD/phase1-dependencies.yaml"

# Phase 1b: blocking Job only.
yq 'select(.kind == "Job" and .metadata.name == strenv(RELEASE) + "-system-update")' \
  "$BUILD/all.yaml" > "$BUILD/phase1-job.yaml"

# Phase 2: workloads and the rest of the render. Every Helm hook Job stays out.
# With the values above, the only hook Jobs are the two system-update Jobs.
yq 'select(
      .kind != "Job"
      or (.metadata.annotations["helm.sh/hook"] == null)
    )' "$BUILD/all.yaml" > "$BUILD/phase2.yaml"

# Phase 3: non-blocking Job only.
yq 'select(.kind == "Job" and .metadata.name == strenv(RELEASE) + "-system-update-nonblk")' \
  "$BUILD/all.yaml" > "$BUILD/phase3.yaml"
```

If the blocking Job depends on another object, such as a NetworkPolicy or an ExternalSecret, add it to `phase1-dependencies.yaml` so it is applied before the Job.

`kubectl apply` of one file creates every object in that file together. Keep the blocking Job in its own file and apply it only after the dependency apply has returned.

Check the Job conditions, not the pod counts. `status.failed` counts failed pods. The system-update Jobs use `restartPolicy: Never`, so Kubernetes retries a failed pod up to `backoffLimit`, and that counter is 1 after the first retry while the Job may still succeed. The `Failed` condition is set only when retries are exhausted.

```bash
# Exit on the Job's Complete or Failed condition. kubectl wait --for=condition=complete
# keeps running until the timeout when the Job fails, because Complete never appears.
wait_for_job() {
  job="$1"
  deadline=$((SECONDS + 10800)) # 180 minutes; use a timeout that covers a full index build
  while true; do
    complete=$(kubectl -n "$NAMESPACE" get "job/${job}" \
      -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}')
    failed=$(kubectl -n "$NAMESPACE" get "job/${job}" \
      -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}')
    if [ "$complete" = "True" ]; then
      return 0
    fi
    if [ "$failed" = "True" ]; then
      echo "Job ${job} failed" >&2
      kubectl -n "$NAMESPACE" logs "job/${job}" >&2 || true
      return 1
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "Timed out waiting for Job ${job}" >&2
      return 1
    fi
    sleep 15
  done
}
```

### Phase 1: blocking system-update

Apply the dependency file first. When that command returns, the Secrets, ConfigMaps, and RBAC exist. Then delete any previous blocking Job and apply `phase1-job.yaml`. The container args are `-u SystemUpdateBlocking`. Do not apply Deployments in this phase, and do not apply `<release>-system-update-nonblk`.

This order matters on a new install, where those objects are not already in the cluster. On an upgrade, applying them again is safe, and the Job still starts only after that apply returns.

```bash
kubectl -n "$NAMESPACE" apply -f "$BUILD/phase1-dependencies.yaml"
kubectl -n "$NAMESPACE" delete job "${RELEASE}-system-update" --ignore-not-found
kubectl -n "$NAMESPACE" apply -f "$BUILD/phase1-job.yaml"
wait_for_job "${RELEASE}-system-update"
```

Stop when `wait_for_job` returns an error. Do not roll out new components after a failed blocking Job.

### Phase 2: components

Apply phase 2: GMS, the frontend, MAE, MCE, actions, Services, and CronJobs. `helm upgrade` applies these objects after the pre-upgrade hook and before the post-upgrade hook. A default `helm upgrade` does not wait for the new pods to become Ready before that post-upgrade hook unless you pass `--wait`.

Wait here until the new GMS, MAE, and MCE pods are Ready. Phase 3 calls the live GMS.

```bash
kubectl -n "$NAMESPACE" apply -f "$BUILD/phase2.yaml"
kubectl -n "$NAMESPACE" get deploy -l "app.kubernetes.io/instance=${RELEASE}" -o name \
  | xargs -n1 kubectl -n "$NAMESPACE" rollout status
```

### Phase 3: non-blocking system-update

Apply only `<release>-system-update-nonblk`. The container args are `-u SystemUpdateNonBlocking`. This is the live sweep, including `MigrateAspects` and Elasticsearch catch-up. Skipping it leaves a ZDU upgrade unfinished.

```bash
kubectl -n "$NAMESPACE" delete job "${RELEASE}-system-update-nonblk" --ignore-not-found
kubectl -n "$NAMESPACE" apply -f "$BUILD/phase3.yaml"
wait_for_job "${RELEASE}-system-update-nonblk"
```

Stop when `wait_for_job` returns an error, the same way as phase 1.

The same three phases apply to a new install and to an upgrade. SQL, Kafka, and Elasticsearch or OpenSearch must already be up.

## Spinnaker

On every pipeline run, use a **Bake (Manifest)** stage with the Helm renderer. Point it at the chart artifact and the values artifact. Do not store a pre-rendered manifest as the pipeline input.

Split that baked manifest with the `yq` filters above. The split files are pipeline artifacts, produced on that execution.

Then Deploy Manifest stages, with a wait between them:

1. Deploy `phase1-dependencies.yaml`. After that stage finishes, deploy `phase1-job.yaml`. Wait until `<release>-system-update` reports the `Complete` condition, and fail the pipeline as soon as it reports the `Failed` condition.
2. Deploy phase 2. Wait until GMS, MAE, and MCE are Ready. A default `helm upgrade` does not wait for Ready before the post-upgrade hook unless you pass `--wait`. This wait is required here because phase 3 calls the live GMS.
3. Deploy phase 3. Wait until `<release>-system-update-nonblk` succeeds, and fail the pipeline as soon as that Job fails.

Jobs are immutable. Delete the previous Job before the deploy stage, or set `strategy.spinnaker.io/replace: "true"` on the two Job manifests with a pipeline override applied to the fresh bake.

## Kustomize

Kustomize applies every resource in one kustomization together. It does not wait for the blocking Job before it creates Deployments.

Each deploy should:

1. Run `helm template` into a gitignored directory.
2. Split that output into `phase1-dependencies.yaml`, `phase1-job.yaml`, `phase2.yaml`, and `phase3.yaml`.
3. `kubectl apply -k` those kustomizations in that order. Apply the dependency kustomization and wait for it to return before applying the blocking Job. Use `wait_for_job` after the blocking Job and after the non-blocking Job, and wait for GMS, MAE, and MCE to be Ready after phase 2.

Commit the kustomizations and patches. Regenerate the rendered and split files every time.

```yaml
# phase1-dependencies/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - rendered/phase1-dependencies.yaml # generated on each deploy; gitignore this file
```

Repeat that for the blocking Job, phase 2, and phase 3. Put patches next to the kustomization and target them by kind and name, for example Job `datahub-system-update`.

Kustomize can also render the chart itself:

```yaml
helmCharts:
  - name: datahub
    repo: https://helm.datahubproject.io
    version: <chart-version>
    releaseName: datahub
    namespace: datahub
    valuesFile: values.yaml
```

`kubectl kustomize --enable-helm` runs that render on each build. You still split the result and apply the dependency file, the blocking Job, the workloads, and the non-blocking Job as separate steps. One kustomization that includes the whole chart does not order the Jobs.
