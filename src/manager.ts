import * as k8s from '@kubernetes/client-node';
import { parseEnvContent } from '@zyno-io/config';

import { K8sClient } from './k8s';
import { createLogger } from './logger';

const INITIAL_RETRY_DELAY = 1_000;
const MAX_RETRY_DELAY = 30_000;

export class Manager {
    private logger = createLogger('Manager');
    private kubeWatch: k8s.Watch;
    private isReady = false;
    private isPendingSync = true;
    private cache: { [key: string]: { secret?: k8s.V1Secret; configMap?: k8s.V1ConfigMap } } = {};
    private syncInterval?: ReturnType<typeof setInterval>;
    private configMapAbortController?: AbortController;
    private secretAbortController?: AbortController;
    private stopped = false;
    private configMapRetryDelay = INITIAL_RETRY_DELAY;
    private secretRetryDelay = INITIAL_RETRY_DELAY;
    private configMapResourceVersion?: string;
    private secretResourceVersion?: string;

    constructor(private k8sClient: K8sClient) {
        this.kubeWatch = new k8s.Watch(this.k8sClient.kubeConfig);
        this.logger.info('Manager created');
    }

    async start() {
        await this.watchConfigMaps();
        await this.watchSecrets();

        await new Promise(resolve => setTimeout(resolve, 5_000));
        this.isReady = true;
        this.syncSecrets();

        this.syncInterval = setInterval(() => this.syncSecrets(), 30_000);
    }

    stop() {
        this.stopped = true;
        this.configMapAbortController?.abort();
        this.secretAbortController?.abort();
        if (this.syncInterval) {
            clearInterval(this.syncInterval);
        }
    }

    async watchConfigMaps() {
        this.logger.info('Starting ConfigMap watch');
        const queryParams: Record<string, string> = {
            labelSelector: 'config.s24.dev/target-secret'
        };
        if (this.configMapResourceVersion) {
            queryParams.resourceVersion = this.configMapResourceVersion;
        }
        this.configMapAbortController = await this.kubeWatch.watch(
            '/api/v1/configmaps',
            queryParams,
            (type: string, configMap: k8s.V1ConfigMap) => {
                this.configMapRetryDelay = INITIAL_RETRY_DELAY;
                this.logger.info(`ConfigMap ${configMap.metadata?.name} ${type}`);
                if (configMap.metadata?.resourceVersion) {
                    this.configMapResourceVersion = configMap.metadata.resourceVersion;
                }
                const secretName = `${configMap.metadata?.namespace}/${configMap.metadata?.labels?.['config.s24.dev/target-secret']}`;

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
                    this.configMapResourceVersion = undefined;
                }
                const delay = this.configMapRetryDelay;
                this.configMapRetryDelay = Math.min(this.configMapRetryDelay * 2, MAX_RETRY_DELAY);
                setTimeout(() => this.watchConfigMaps(), delay);
            }
        );
    }

    async watchSecrets() {
        this.logger.info('Starting Secret watch');
        const queryParams: Record<string, string> = {
            labelSelector: 'config.s24.dev/source-configmap'
        };
        if (this.secretResourceVersion) {
            queryParams.resourceVersion = this.secretResourceVersion;
        }
        this.secretAbortController = await this.kubeWatch.watch(
            '/api/v1/secrets',
            queryParams,
            (type: string, secret: k8s.V1Secret) => {
                this.secretRetryDelay = INITIAL_RETRY_DELAY;
                this.logger.info(`Secret ${secret.metadata?.name} ${type}`);
                if (secret.metadata?.resourceVersion) {
                    this.secretResourceVersion = secret.metadata.resourceVersion;
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
                    this.secretResourceVersion = undefined;
                }
                const delay = this.secretRetryDelay;
                this.secretRetryDelay = Math.min(this.secretRetryDelay * 2, MAX_RETRY_DELAY);
                setTimeout(() => this.watchSecrets(), delay);
            }
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
            } else if (secret.metadata!.labels?.['config.s24.dev/source-configmap-version'] !== configMap!.metadata!.resourceVersion) {
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
        const sourceKey = configMap.metadata?.labels?.['config.s24.dev/source-key'] ?? '.env';
        const sourceData = configMap.data?.[sourceKey];
        if (sourceData === undefined) {
            throw new Error(`Key ${sourceKey} not found in ConfigMap ${configMap.metadata?.name}`);
        }

        const keySecretName = configMap.metadata?.labels?.['config.s24.dev/decryption-secret'];
        const keySecretKey = configMap.metadata?.labels?.['config.s24.dev/decryption-secret-key'] ?? 'CONFIG_DECRYPTION_KEY';
        const decryptionSecret = keySecretName
            ? await this.getDecryptionSecret(configMap.metadata!.namespace!, keySecretName, keySecretKey)
            : undefined;

        const targetSecret = configMap.metadata!.labels!['config.s24.dev/target-secret'];

        const secretData = await this.extractConfigFromEncryptedEnv(sourceData, decryptionSecret);
        const secret = await this.createSecretWithConfigMapData(configMap, targetSecret, secretData, existingSecret);

        return secret;
    }

    private async getDecryptionSecret(sourceNs: string, keySecretName: string, keySecretKey: string): Promise<string> {
        // TODO: There's a security risk involved here. Figure out how to make this more secure. Maybe require an annotation on the source secret?
        // const [secretNs, secretName] = keySecretName.includes('/') ? keySecretName.split('/') : [sourceNs, keySecretName];
        const [secretNs, secretName] = [sourceNs, keySecretName];
        const secret = await this.k8sClient.coreV1Api.readNamespacedSecret({
            name: secretName,
            namespace: secretNs
        });
        if (secret.data?.[keySecretKey] === undefined) {
            throw new Error(`Key ${keySecretKey} not found in secret ${keySecretName}`);
        }
        return Buffer.from(secret.data[keySecretKey], 'base64').toString('utf-8');
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
                    'config.s24.dev/source-configmap': configMap.metadata!.name!,
                    'config.s24.dev/source-configmap-version': configMap.metadata!.resourceVersion!
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
