import * as k8s from '@kubernetes/client-node';
import { parseEnvContent } from '@zyno-io/config';

import { K8sClient } from './k8s';
import { createLogger } from './logger';

const INITIAL_RETRY_DELAY = 1_000;
const MAX_RETRY_DELAY = 30_000;
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
    private configMapResourceVersions = new Map<string, string>();
    private secretResourceVersions = new Map<string, string>();

    constructor(private k8sClient: K8sClient) {
        this.kubeWatch = new k8s.Watch(this.k8sClient.kubeConfig);
        this.logger.info('Manager created');
    }

    async start() {
        for (const labelSelector of CONFIG_MAP_LABEL_SELECTORS) {
            await this.watchConfigMaps(labelSelector);
        }
        for (const labelSelector of SECRET_LABEL_SELECTORS) {
            await this.watchSecrets(labelSelector);
        }

        await new Promise(resolve => setTimeout(resolve, 5_000));
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

    async watchConfigMaps(labelSelector: string = CONFIG_LABELS.targetSecret) {
        this.logger.info(`Starting ConfigMap watch for ${labelSelector}`);
        const queryParams: Record<string, string> = {
            labelSelector
        };
        const resourceVersion = this.configMapResourceVersions.get(labelSelector);
        if (resourceVersion) {
            queryParams.resourceVersion = resourceVersion;
        }
        this.configMapAbortControllers.set(
            labelSelector,
            await this.kubeWatch.watch(
                '/api/v1/configmaps',
                queryParams,
                (type: string, configMap: k8s.V1ConfigMap) => {
                    this.configMapRetryDelays.set(labelSelector, INITIAL_RETRY_DELAY);
                    this.logger.info(`ConfigMap ${configMap.metadata?.name} ${type}`);
                    if (configMap.metadata?.resourceVersion) {
                        this.configMapResourceVersions.set(labelSelector, configMap.metadata.resourceVersion);
                    }
                    const targetSecret = getConfigLabel(configMap.metadata, 'targetSecret');
                    if (!targetSecret) {
                        this.logger.info(`ConfigMap ${configMap.metadata?.name} does not declare a target secret. Skipping.`);
                        return;
                    }
                    const secretName = `${configMap.metadata?.namespace}/${targetSecret}`;

                    if (type === 'DELETED') {
                        this.cache[secretName] = { ...this.cache[secretName], configMap: undefined };
                    } else {
                        this.cache[secretName] = { ...this.cache[secretName], configMap };
                    }

                    this.syncSecrets();
                },
                err => {
                    if (this.stopped) return;
                    this.logger.error({ err }, 'Failed to watch ConfigMaps');
                    if (err?.code === 410) {
                        this.configMapResourceVersions.delete(labelSelector);
                    }
                    const delay = this.configMapRetryDelays.get(labelSelector) ?? INITIAL_RETRY_DELAY;
                    this.configMapRetryDelays.set(labelSelector, Math.min(delay * 2, MAX_RETRY_DELAY));
                    setTimeout(() => this.watchConfigMaps(labelSelector), delay);
                }
            )
        );
    }

    async watchSecrets(labelSelector: string = CONFIG_LABELS.sourceConfigMap) {
        this.logger.info(`Starting Secret watch for ${labelSelector}`);
        const queryParams: Record<string, string> = {
            labelSelector
        };
        const resourceVersion = this.secretResourceVersions.get(labelSelector);
        if (resourceVersion) {
            queryParams.resourceVersion = resourceVersion;
        }
        this.secretAbortControllers.set(
            labelSelector,
            await this.kubeWatch.watch(
                '/api/v1/secrets',
                queryParams,
                (type: string, secret: k8s.V1Secret) => {
                    this.secretRetryDelays.set(labelSelector, INITIAL_RETRY_DELAY);
                    this.logger.info(`Secret ${secret.metadata?.name} ${type}`);
                    if (secret.metadata?.resourceVersion) {
                        this.secretResourceVersions.set(labelSelector, secret.metadata.resourceVersion);
                    }
                    const secretName = `${secret.metadata?.namespace}/${secret.metadata?.name}`;

                    if (type === 'DELETED') {
                        this.cache[secretName] = { ...this.cache[secretName], secret: undefined };
                    } else {
                        this.cache[secretName] = { ...this.cache[secretName], secret };
                    }

                    this.syncSecrets();
                },
                err => {
                    if (this.stopped) return;
                    this.logger.error({ err }, 'Failed to watch Secrets');
                    if (err?.code === 410) {
                        this.secretResourceVersions.delete(labelSelector);
                    }
                    const delay = this.secretRetryDelays.get(labelSelector) ?? INITIAL_RETRY_DELAY;
                    this.secretRetryDelays.set(labelSelector, Math.min(delay * 2, MAX_RETRY_DELAY));
                    setTimeout(() => this.watchSecrets(labelSelector), delay);
                }
            )
        );
    }

    syncSecrets() {
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
                this.logger.info(`ConfigMap for ${secretName} not found. Deleting secret.`);
                try {
                    await this.k8sClient.coreV1Api.deleteNamespacedSecret({
                        name: secret!.metadata!.name!,
                        namespace: secret!.metadata!.namespace!
                    });
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
