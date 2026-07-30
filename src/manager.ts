import * as k8s from '@kubernetes/client-node';
import { parseEnvContent } from '@zyno-io/config';

import { K8sClient } from './k8s';
import { createLogger } from './logger';

const INITIAL_RETRY_DELAY = 1_000;
const MAX_RETRY_DELAY = 30_000;
/**
 * How long a watch stream must survive before we treat the connection as healthy and
 * forget earlier failures. A successful LIST is deliberately NOT enough: if the LIST works
 * but the watch dies immediately, resetting on the LIST would hold the delay at 1s and hot
 * loop against the API server.
 */
const HEALTHY_WATCH_DURATION = 10_000;
const WATCH_EVENT_DELETED = 'DELETED';
const WATCH_EVENT_BOOKMARK = 'BOOKMARK';
const WATCH_EVENT_ERROR = 'ERROR';
const DEFAULT_DECRYPTION_SECRET_KEY = 'CONFIG_DECRYPTION_SECRET';
const LEGACY_DECRYPTION_SECRET_KEY = 'CONFIG_DECRYPTION_KEY';
const CONFIG_LABEL_PREFIX = 'config.zyno.io';
const LEGACY_CONFIG_LABEL_PREFIX = 'config.s24.dev';
const CONFIG_LABELS = {
    targetSecret: `${CONFIG_LABEL_PREFIX}/target-secret`,
    sourceKey: `${CONFIG_LABEL_PREFIX}/source-key`,
    decryptionSecret: `${CONFIG_LABEL_PREFIX}/decryption-secret`,
    decryptionSecretKey: `${CONFIG_LABEL_PREFIX}/decryption-secret-key`,
    sourceConfigMap: `${CONFIG_LABEL_PREFIX}/source-configmap`,
    sourceConfigMapVersion: `${CONFIG_LABEL_PREFIX}/source-configmap-version`
} as const;
type ConfigLabelName = keyof typeof CONFIG_LABELS;
const LEGACY_CONFIG_LABELS: Record<ConfigLabelName, string> = {
    targetSecret: `${LEGACY_CONFIG_LABEL_PREFIX}/target-secret`,
    sourceKey: `${LEGACY_CONFIG_LABEL_PREFIX}/source-key`,
    decryptionSecret: `${LEGACY_CONFIG_LABEL_PREFIX}/decryption-secret`,
    decryptionSecretKey: `${LEGACY_CONFIG_LABEL_PREFIX}/decryption-secret-key`,
    sourceConfigMap: `${LEGACY_CONFIG_LABEL_PREFIX}/source-configmap`,
    sourceConfigMapVersion: `${LEGACY_CONFIG_LABEL_PREFIX}/source-configmap-version`
};
const CONFIG_MAP_LABEL_SELECTORS = [CONFIG_LABELS.targetSecret, LEGACY_CONFIG_LABELS.targetSecret];
const SECRET_LABEL_SELECTORS = [CONFIG_LABELS.sourceConfigMap, LEGACY_CONFIG_LABELS.sourceConfigMap];

function getConfigLabel(metadata: k8s.V1ObjectMeta | undefined, labelName: ConfigLabelName): string | undefined {
    return metadata?.labels?.[CONFIG_LABELS[labelName]] ?? metadata?.labels?.[LEGACY_CONFIG_LABELS[labelName]];
}

/**
 * A watch stream delivers more than the resource type it was opened for: `BOOKMARK`
 * events carry nothing but a resourceVersion, and `ERROR` events carry a `V1Status`.
 * Neither has the shape the resource handlers expect.
 */
type WatchedObject = k8s.V1ConfigMap | k8s.V1Secret | k8s.V1Status;

export class Manager {
    private logger = createLogger('Manager');
    private kubeWatch: k8s.Watch;
    private isReady = false;
    private isPendingSync = true;
    private cache: { [key: string]: { secret?: k8s.V1Secret; configMap?: k8s.V1ConfigMap } } = {};
    private syncInterval?: ReturnType<typeof setInterval>;
    private configMapAbortControllers = new Map<string, AbortController>();
    private secretAbortControllers = new Map<string, AbortController>();
    private stopped = false;
    private configMapRetryDelays = new Map<string, number>();
    private secretRetryDelays = new Map<string, number>();
    /**
     * Cache keys each label selector saw in its most recent LIST. Pruning is decided
     * against the *union* of these sets, never a single selector's, because one cache slot
     * can legitimately be fed by more than one selector (the modern and legacy label
     * prefixes are watched separately). Erring towards keeping an entry is deliberate: the
     * cost of a late prune is a stale Secret for one resync interval, whereas the cost of
     * an eager prune is deleting a live Secret.
     */
    private configMapKeysBySelector = new Map<string, Set<string>>();
    private secretKeysBySelector = new Map<string, Set<string>>();

    constructor(private k8sClient: K8sClient) {
        this.kubeWatch = new k8s.Watch(this.k8sClient.kubeConfig);
        this.logger.info('Manager created');
    }

    async start() {
        if (this.stopped) return;

        for (const labelSelector of CONFIG_MAP_LABEL_SELECTORS) {
            if (this.stopped) return;
            await this.listAndWatchConfigMaps(labelSelector);
        }
        for (const labelSelector of SECRET_LABEL_SELECTORS) {
            if (this.stopped) return;
            await this.listAndWatchSecrets(labelSelector);
        }

        await new Promise(resolve => setTimeout(resolve, 5_000));
        // `stop()` can run while the startup delay is pending. It is terminal: do not
        // reactivate reconciliation or leave a new interval behind after shutdown.
        if (this.stopped) return;
        this.isReady = true;
        this.syncSecrets();

        this.syncInterval = setInterval(() => this.syncSecrets(), 30_000);
    }

    stop() {
        this.stopped = true;
        for (const abortController of this.configMapAbortControllers.values()) {
            abortController.abort();
        }
        for (const abortController of this.secretAbortControllers.values()) {
            abortController.abort();
        }
        if (this.syncInterval) {
            clearInterval(this.syncInterval);
        }
    }

    /**
     * Handles the two event types that aren't resources. Returns true when the event was
     * one of them and the caller must stop processing it.
     *
     * This has to run before anything resource-shaped, because for these events
     * `object.metadata` is either absent or a `V1ListMeta` — reading a name/namespace off
     * it yields `undefined`, and caching that produces an entry no reconcile can ever
     * address.
     */
    private handleWatchControlEvent(kind: 'ConfigMap' | 'Secret', type: string, object: WatchedObject, labelSelector: string): boolean {
        if (type === WATCH_EVENT_BOOKMARK) {
            // We never resume a watch from a stored resourceVersion (each reconnect
            // re-LISTs), so a bookmark tells us nothing — but it must still be swallowed
            // here so it can't be mistaken for a resource.
            return true;
        }

        if (type === WATCH_EVENT_ERROR) {
            // This is the ONLY place a watch error status is visible: the stream closes
            // immediately afterwards and @kubernetes/client-node's `done` callback then
            // fires with a null error. Recovery needs no special handling because the
            // reconnect re-LISTs — which is precisely why the resourceVersion-resume path
            // this replaced could wedge forever on a single expired version.
            const status = object as k8s.V1Status;
            this.logger.error(
                { status: { code: status?.code, reason: status?.reason, message: status?.message } },
                `${kind} watch for ${labelSelector} returned an error status. Reconnecting with a fresh list.`
            );
            // Deliberately NOT resetting the retry backoff — this is a failed connection,
            // and the `done` callback schedules the reconnect. Resetting here is what
            // turned a backed-off retry into a hot loop.
            return true;
        }

        return false;
    }

    /**
     * Applies a LIST result as the authoritative view for one label selector, dropping
     * cache entries that selector used to contribute and no longer does. Returns the list's
     * resourceVersion, which is the only safe version to start a watch from.
     */
    private applyList<T extends k8s.V1ConfigMap | k8s.V1Secret>(
        kind: 'ConfigMap' | 'Secret',
        labelSelector: string,
        items: T[],
        keysBySelector: Map<string, Set<string>>,
        keyOf: (item: T) => string | undefined,
        apply: (key: string, item: T) => void,
        clear: (key: string) => boolean
    ) {
        const keys = new Set<string>();
        for (const item of items) {
            const key = keyOf(item);
            if (key) keys.add(key);
        }
        keysBySelector.set(labelSelector, keys);

        const stillClaimed = new Set<string>();
        for (const claimed of keysBySelector.values()) {
            for (const key of claimed) stillClaimed.add(key);
        }

        // Anything we hold for this kind that no selector lists any more is gone from the
        // cluster — most likely deleted while we were disconnected, so no DELETED event
        // ever reached us.
        for (const key in this.cache) {
            if (stillClaimed.has(key)) continue;
            // `clear` reports whether it actually held anything, so a cache entry that only
            // ever had the other half of the pair doesn't produce a misleading log line.
            if (clear(key)) {
                this.logger.info(`${kind} for ${key} is gone from the cluster. Dropped it from the cache.`);
            }
        }

        for (const item of items) {
            const key = keyOf(item);
            if (key) apply(key, item);
        }
    }

    /** Removes one selector's claim and reports whether another selector still owns the cache key. */
    private removeSelectorClaim(keysBySelector: Map<string, Set<string>>, labelSelector: string, key: string): boolean {
        keysBySelector.get(labelSelector)?.delete(key);
        for (const claimed of keysBySelector.values()) {
            if (claimed.has(key)) return true;
        }
        return false;
    }

    async listAndWatchConfigMaps(labelSelector: string = CONFIG_LABELS.targetSecret) {
        // A retry timer may fire after stop(); it must not issue a fresh LIST or trigger
        // reconciliation after shutdown.
        if (this.stopped) return;
        const resourceVersion = await this.listConfigMaps(labelSelector);
        if (resourceVersion === undefined) return;
        // stop() may have landed while the LIST was in flight; opening a watch now would
        // outlive the abort controllers it already drained.
        if (this.stopped) return;
        await this.watchConfigMaps(labelSelector, resourceVersion);
    }

    /** Returns the list resourceVersion, or undefined if the LIST failed and a retry is queued. */
    private async listConfigMaps(labelSelector: string): Promise<string | undefined> {
        this.logger.info(`Listing ConfigMaps for ${labelSelector}`);
        let list: k8s.V1ConfigMapList;
        try {
            list = await this.k8sClient.coreV1Api.listConfigMapForAllNamespaces({ labelSelector });
        } catch (err) {
            this.logger.error({ err }, `Failed to list ConfigMaps for ${labelSelector}`);
            this.retryConfigMaps(labelSelector);
            return undefined;
        }

        this.applyList(
            'ConfigMap',
            labelSelector,
            list.items ?? [],
            this.configMapKeysBySelector,
            configMap => this.configMapCacheKey(configMap),
            (key, configMap) => {
                this.cache[key] = { ...this.cache[key], configMap };
            },
            key => {
                if (!this.cache[key]?.configMap) return false;
                this.cache[key] = { ...this.cache[key], configMap: undefined };
                return true;
            }
        );

        this.syncSecrets();

        return list.metadata?.resourceVersion;
    }

    private configMapCacheKey(configMap: k8s.V1ConfigMap): string | undefined {
        const targetSecret = getConfigLabel(configMap.metadata, 'targetSecret');
        if (!targetSecret) {
            this.logger.info(`ConfigMap ${configMap.metadata?.name} does not declare a target secret. Skipping.`);
            return undefined;
        }
        const namespace = configMap.metadata?.namespace;
        if (!namespace) {
            this.logger.error(`ConfigMap ${configMap.metadata?.name} has no namespace. Skipping.`);
            return undefined;
        }
        return `${namespace}/${targetSecret}`;
    }

    private retryConfigMaps(labelSelector: string) {
        if (this.stopped) return;
        const delay = this.configMapRetryDelays.get(labelSelector) ?? INITIAL_RETRY_DELAY;
        this.configMapRetryDelays.set(labelSelector, Math.min(delay * 2, MAX_RETRY_DELAY));
        setTimeout(() => this.listAndWatchConfigMaps(labelSelector), delay);
    }

    private retrySecrets(labelSelector: string) {
        if (this.stopped) return;
        const delay = this.secretRetryDelays.get(labelSelector) ?? INITIAL_RETRY_DELAY;
        this.secretRetryDelays.set(labelSelector, Math.min(delay * 2, MAX_RETRY_DELAY));
        setTimeout(() => this.listAndWatchSecrets(labelSelector), delay);
    }

    async watchConfigMaps(labelSelector: string = CONFIG_LABELS.targetSecret, resourceVersion?: string) {
        if (this.stopped) return;
        this.logger.info(`Starting ConfigMap watch for ${labelSelector}`);
        const startedAt = Date.now();
        const queryParams: Record<string, string> = { labelSelector };
        if (resourceVersion) {
            queryParams.resourceVersion = resourceVersion;
        }
        const abortController = await this.kubeWatch.watch(
            '/api/v1/configmaps',
            queryParams,
            (type: string, configMap: k8s.V1ConfigMap) => {
                if (this.stopped || this.handleWatchControlEvent('ConfigMap', type, configMap, labelSelector)) {
                    return;
                }

                this.configMapRetryDelays.set(labelSelector, INITIAL_RETRY_DELAY);
                this.logger.info(`ConfigMap ${configMap.metadata?.name} ${type}`);
                const secretName = this.configMapCacheKey(configMap);
                if (!secretName) return;

                if (type === WATCH_EVENT_DELETED) {
                    // The same resource can be seen through both the modern and legacy
                    // label selectors. Watch streams have no cross-stream ordering, so a
                    // deletion from one selector must not evict a resource the other still
                    // claims (for example while labels are being migrated).
                    if (!this.removeSelectorClaim(this.configMapKeysBySelector, labelSelector, secretName)) {
                        this.cache[secretName] = { ...this.cache[secretName], configMap: undefined };
                    }
                } else {
                    this.cache[secretName] = { ...this.cache[secretName], configMap };
                    this.configMapKeysBySelector.get(labelSelector)?.add(secretName);
                }

                this.syncSecrets();
            },
            err => {
                if (this.stopped) return;
                this.logger.error({ err }, 'Failed to watch ConfigMaps');
                if (Date.now() - startedAt >= HEALTHY_WATCH_DURATION) {
                    this.configMapRetryDelays.set(labelSelector, INITIAL_RETRY_DELAY);
                }
                this.retryConfigMaps(labelSelector);
            }
        );
        // stop() can land while opening the HTTP connection, after it has already aborted
        // the controllers it knew about. Abort this late controller rather than leaking it.
        if (this.stopped) {
            abortController.abort();
            return;
        }
        this.configMapAbortControllers.set(labelSelector, abortController);
    }

    async listAndWatchSecrets(labelSelector: string = CONFIG_LABELS.sourceConfigMap) {
        if (this.stopped) return;
        const resourceVersion = await this.listSecrets(labelSelector);
        if (resourceVersion === undefined) return;
        // See listAndWatchConfigMaps: don't open a watch after stop().
        if (this.stopped) return;
        await this.watchSecrets(labelSelector, resourceVersion);
    }

    /** Returns the list resourceVersion, or undefined if the LIST failed and a retry is queued. */
    private async listSecrets(labelSelector: string): Promise<string | undefined> {
        this.logger.info(`Listing Secrets for ${labelSelector}`);
        let list: k8s.V1SecretList;
        try {
            list = await this.k8sClient.coreV1Api.listSecretForAllNamespaces({ labelSelector });
        } catch (err) {
            this.logger.error({ err }, `Failed to list Secrets for ${labelSelector}`);
            this.retrySecrets(labelSelector);
            return undefined;
        }

        this.applyList(
            'Secret',
            labelSelector,
            list.items ?? [],
            this.secretKeysBySelector,
            secret => this.secretCacheKey(secret),
            (key, secret) => {
                this.cache[key] = { ...this.cache[key], secret };
            },
            key => {
                if (!this.cache[key]?.secret) return false;
                this.cache[key] = { ...this.cache[key], secret: undefined };
                return true;
            }
        );

        this.syncSecrets();

        return list.metadata?.resourceVersion;
    }

    private secretCacheKey(secret: k8s.V1Secret): string | undefined {
        const name = secret.metadata?.name;
        const namespace = secret.metadata?.namespace;
        if (!name || !namespace) {
            this.logger.error('Ignoring Secret with no name or namespace.');
            return undefined;
        }
        return `${namespace}/${name}`;
    }

    async watchSecrets(labelSelector: string = CONFIG_LABELS.sourceConfigMap, resourceVersion?: string) {
        if (this.stopped) return;
        this.logger.info(`Starting Secret watch for ${labelSelector}`);
        const startedAt = Date.now();
        const queryParams: Record<string, string> = { labelSelector };
        if (resourceVersion) {
            queryParams.resourceVersion = resourceVersion;
        }
        const abortController = await this.kubeWatch.watch(
            '/api/v1/secrets',
            queryParams,
            (type: string, secret: k8s.V1Secret) => {
                if (this.stopped || this.handleWatchControlEvent('Secret', type, secret, labelSelector)) {
                    return;
                }

                this.secretRetryDelays.set(labelSelector, INITIAL_RETRY_DELAY);
                this.logger.info(`Secret ${secret.metadata?.name} ${type}`);
                const secretName = this.secretCacheKey(secret);
                if (!secretName) return;

                if (type === WATCH_EVENT_DELETED) {
                    if (!this.removeSelectorClaim(this.secretKeysBySelector, labelSelector, secretName)) {
                        this.cache[secretName] = { ...this.cache[secretName], secret: undefined };
                    }
                } else {
                    this.cache[secretName] = { ...this.cache[secretName], secret };
                    this.secretKeysBySelector.get(labelSelector)?.add(secretName);
                }

                this.syncSecrets();
            },
            err => {
                if (this.stopped) return;
                this.logger.error({ err }, 'Failed to watch Secrets');
                if (Date.now() - startedAt >= HEALTHY_WATCH_DURATION) {
                    this.secretRetryDelays.set(labelSelector, INITIAL_RETRY_DELAY);
                }
                this.retrySecrets(labelSelector);
            }
        );
        if (this.stopped) {
            abortController.abort();
            return;
        }
        this.secretAbortControllers.set(labelSelector, abortController);
    }

    syncSecrets() {
        if (this.stopped) return;
        if (!this.isReady) {
            this.isPendingSync = true;
            return;
        }

        this.isReady = false;
        this.isPendingSync = false;

        this._syncSecrets()
            .then(() => {
                this.logger.info('Secrets synced');
            })
            .catch(err => {
                this.logger.error({ err }, 'Failed to sync secrets');
            })
            .finally(() => {
                if (this.stopped) return;
                this.isReady = true;
                if (this.isPendingSync) setTimeout(() => this.syncSecrets(), 0);
            });
    }

    private async _syncSecrets() {
        for (const secretName in this.cache) {
            const { secret, configMap } = this.cache[secretName];

            if (!secret && !configMap) {
                delete this.cache[secretName];
                continue;
            }

            if (!configMap) {
                // An entry we can't address can never be reconciled, and this branch would
                // otherwise ask the API server to delete a secret with no name — which
                // throws, hits the `continue` below, and leaves the entry to be retried
                // forever. Drop it instead.
                const name = secret?.metadata?.name;
                const namespace = secret?.metadata?.namespace;
                if (!name || !namespace) {
                    this.logger.error(`Cached entry ${secretName} has no addressable secret. Dropping it.`);
                    delete this.cache[secretName];
                    continue;
                }

                this.logger.info(`ConfigMap for ${secretName} not found. Deleting secret.`);
                try {
                    await this.k8sClient.coreV1Api.deleteNamespacedSecret({ name, namespace });
                } catch (err: unknown) {
                    if ((err as { code?: number })?.code !== 404) {
                        this.logger.error({ err }, `Failed to delete secret ${secretName}`);
                        continue;
                    }
                }
                delete this.cache[secretName];
                continue;
            }

            if (!secret) {
                this.logger.info(`Secret for ${secretName} does not exist. Creating secret.`);
                try {
                    this.cache[secretName].secret = await this.createSecretForConfigMap(configMap!);
                } catch (err) {
                    this.logger.error({ err }, `Failed to create secret ${secretName}`);
                }
            } else if (getConfigLabel(secret.metadata, 'sourceConfigMapVersion') !== configMap!.metadata!.resourceVersion) {
                this.logger.info(`ConfigMap for ${secretName} updated. Updating secret.`);
                try {
                    this.cache[secretName].secret = await this.createSecretForConfigMap(configMap!, secret);
                } catch (err) {
                    this.logger.error({ err }, `Failed to update secret ${secretName}`);
                }
            }
        }
    }

    private async createSecretForConfigMap(configMap: k8s.V1ConfigMap, existingSecret?: k8s.V1Secret): Promise<k8s.V1Secret> {
        const sourceKey = getConfigLabel(configMap.metadata, 'sourceKey') ?? '.env';
        const sourceData = configMap.data?.[sourceKey];
        if (sourceData === undefined) {
            throw new Error(`Key ${sourceKey} not found in ConfigMap ${configMap.metadata?.name}`);
        }

        const keySecretName = getConfigLabel(configMap.metadata, 'decryptionSecret');
        const keySecretKey = getConfigLabel(configMap.metadata, 'decryptionSecretKey');
        const decryptionSecret = keySecretName
            ? await this.getDecryptionSecret(configMap.metadata!.namespace!, keySecretName, keySecretKey)
            : undefined;

        const targetSecret = getConfigLabel(configMap.metadata, 'targetSecret')!;

        const secretData = await this.extractConfigFromEncryptedEnv(sourceData, decryptionSecret);
        const secret = await this.createSecretWithConfigMapData(configMap, targetSecret, secretData, existingSecret);

        return secret;
    }

    private async getDecryptionSecret(sourceNs: string, keySecretName: string, keySecretKey?: string): Promise<string> {
        // TODO: There's a security risk involved here. Figure out how to make this more secure. Maybe require an annotation on the source secret?
        // const [secretNs, secretName] = keySecretName.includes('/') ? keySecretName.split('/') : [sourceNs, keySecretName];
        const [secretNs, secretName] = [sourceNs, keySecretName];
        const secret = await this.k8sClient.coreV1Api.readNamespacedSecret({
            name: secretName,
            namespace: secretNs
        });
        const selectedKey =
            keySecretKey ??
            (secret.data?.[DEFAULT_DECRYPTION_SECRET_KEY] !== undefined ? DEFAULT_DECRYPTION_SECRET_KEY : LEGACY_DECRYPTION_SECRET_KEY);
        const encodedSecret = secret.data?.[selectedKey];
        if (encodedSecret === undefined) {
            const expectedKey = keySecretKey ?? `${DEFAULT_DECRYPTION_SECRET_KEY} or ${LEGACY_DECRYPTION_SECRET_KEY}`;
            throw new Error(`Key ${expectedKey} not found in secret ${keySecretName}`);
        }
        return Buffer.from(encodedSecret, 'base64').toString('utf-8');
    }

    private async extractConfigFromEncryptedEnv(data: string, decryptionSecret?: string): Promise<{ [key: string]: string }> {
        return parseEnvContent(data, decryptionSecret);
    }

    private async createSecretWithConfigMapData(
        configMap: k8s.V1ConfigMap,
        name: string,
        data: { [key: string]: string },
        existingSecret?: k8s.V1Secret
    ): Promise<k8s.V1Secret> {
        const secret: k8s.V1Secret = {
            apiVersion: 'v1',
            kind: 'Secret',
            metadata: {
                name,
                namespace: configMap.metadata!.namespace!,
                labels: {
                    [CONFIG_LABELS.sourceConfigMap]: configMap.metadata!.name!,
                    [CONFIG_LABELS.sourceConfigMapVersion]: configMap.metadata!.resourceVersion!,
                    [LEGACY_CONFIG_LABELS.sourceConfigMap]: configMap.metadata!.name!,
                    [LEGACY_CONFIG_LABELS.sourceConfigMapVersion]: configMap.metadata!.resourceVersion!
                }
            },
            type: 'Opaque',
            data: {}
        };

        for (const key in data) {
            secret.data![key] = Buffer.from(data[key]).toString('base64');
        }

        const result = existingSecret
            ? await this.k8sClient.coreV1Api.replaceNamespacedSecret({
                  name: secret.metadata!.name!,
                  namespace: secret.metadata!.namespace!,
                  body: secret
              })
            : await this.k8sClient.coreV1Api.createNamespacedSecret({ namespace: secret.metadata!.namespace!, body: secret });
        return result;
    }
}
