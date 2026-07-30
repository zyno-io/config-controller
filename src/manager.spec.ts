import type * as k8s from '@kubernetes/client-node';

import type { K8sClient } from './k8s';

const CONFIG_LABELS = {
    targetSecret: 'config.zyno.io/target-secret',
    sourceKey: 'config.zyno.io/source-key',
    decryptionSecret: 'config.zyno.io/decryption-secret',
    decryptionSecretKey: 'config.zyno.io/decryption-secret-key',
    sourceConfigMap: 'config.zyno.io/source-configmap',
    sourceConfigMapVersion: 'config.zyno.io/source-configmap-version'
};
const LEGACY_CONFIG_LABELS = {
    targetSecret: 'config.s24.dev/target-secret',
    sourceKey: 'config.s24.dev/source-key',
    decryptionSecret: 'config.s24.dev/decryption-secret',
    decryptionSecretKey: 'config.s24.dev/decryption-secret-key',
    sourceConfigMap: 'config.s24.dev/source-configmap',
    sourceConfigMapVersion: 'config.s24.dev/source-configmap-version'
};

const mockParseEnvContent = jest.fn<Promise<Record<string, string>>, [string, string | undefined]>();
const mockWatch = jest.fn();
const mockLogger = {
    info: jest.fn(),
    error: jest.fn()
};

jest.mock('@zyno-io/config', () => ({
    parseEnvContent: mockParseEnvContent
}));

jest.mock('@kubernetes/client-node', () => ({
    Watch: jest.fn().mockImplementation(() => ({
        watch: mockWatch
    }))
}));

jest.mock('./logger', () => ({
    createLogger: () => mockLogger
}));

// Must import Manager after mocks are set up
// eslint-disable-next-line @typescript-eslint/no-require-imports
const { Manager } = require('./manager');

describe('Manager', () => {
    let manager: InstanceType<typeof Manager>;
    let mockK8sClient: jest.Mocked<K8sClient>;
    let mockCoreV1Api: {
        deleteNamespacedSecret: jest.Mock;
        createNamespacedSecret: jest.Mock;
        replaceNamespacedSecret: jest.Mock;
        readNamespacedSecret: jest.Mock;
        listConfigMapForAllNamespaces: jest.Mock;
        listSecretForAllNamespaces: jest.Mock;
    };
    let mockAbortController: { abort: jest.Mock };

    beforeEach(() => {
        jest.clearAllMocks();
        jest.useFakeTimers();
        mockWatch.mockReset();
        mockAbortController = { abort: jest.fn() };
        mockWatch.mockResolvedValue(mockAbortController);
        mockParseEnvContent.mockReset();
        mockLogger.info.mockReset();
        mockLogger.error.mockReset();

        mockCoreV1Api = {
            deleteNamespacedSecret: jest.fn(),
            createNamespacedSecret: jest.fn(),
            replaceNamespacedSecret: jest.fn(),
            readNamespacedSecret: jest.fn(),
            // Each (re)connect is a LIST followed by a watch from the list's
            // resourceVersion, so every test that calls start() goes through these.
            listConfigMapForAllNamespaces: jest.fn().mockResolvedValue({ items: [], metadata: { resourceVersion: '100' } }),
            listSecretForAllNamespaces: jest.fn().mockResolvedValue({ items: [], metadata: { resourceVersion: '100' } })
        };

        mockK8sClient = {
            kubeConfig: {},
            coreV1Api: mockCoreV1Api
        } as unknown as jest.Mocked<K8sClient>;

        manager = new Manager(mockK8sClient);
    });

    afterEach(() => {
        jest.useRealTimers();
    });

    const createConfigMap = (
        name: string,
        namespace: string,
        targetSecret: string,
        resourceVersion: string,
        data: Record<string, string> = { '.env': 'KEY=value' }
    ): k8s.V1ConfigMap => ({
        apiVersion: 'v1',
        kind: 'ConfigMap',
        metadata: {
            name,
            namespace,
            resourceVersion,
            labels: {
                [CONFIG_LABELS.targetSecret]: targetSecret
            }
        },
        data
    });

    const createSecret = (name: string, namespace: string, sourceConfigMap: string, sourceVersion: string): k8s.V1Secret => ({
        apiVersion: 'v1',
        kind: 'Secret',
        metadata: {
            name,
            namespace,
            labels: {
                [CONFIG_LABELS.sourceConfigMap]: sourceConfigMap,
                [CONFIG_LABELS.sourceConfigMapVersion]: sourceVersion
            }
        },
        type: 'Opaque',
        data: {}
    });

    describe('constructor', () => {
        it('should create a manager instance', () => {
            expect(manager).toBeInstanceOf(Manager);
        });
    });

    describe('start', () => {
        it('should set up watches and start syncing after 5 seconds', async () => {
            const startPromise = manager.start();

            // Fast-forward through the 5 second initial wait
            await jest.advanceTimersByTimeAsync(5000);
            await startPromise;

            expect(mockWatch).toHaveBeenCalledTimes(4);
            expect(mockWatch).toHaveBeenCalledWith(
                '/api/v1/configmaps',
                { labelSelector: CONFIG_LABELS.targetSecret, resourceVersion: '100' },
                expect.any(Function),
                expect.any(Function)
            );
            expect(mockWatch).toHaveBeenCalledWith(
                '/api/v1/configmaps',
                { labelSelector: LEGACY_CONFIG_LABELS.targetSecret, resourceVersion: '100' },
                expect.any(Function),
                expect.any(Function)
            );
            expect(mockWatch).toHaveBeenCalledWith(
                '/api/v1/secrets',
                { labelSelector: CONFIG_LABELS.sourceConfigMap, resourceVersion: '100' },
                expect.any(Function),
                expect.any(Function)
            );
            expect(mockWatch).toHaveBeenCalledWith(
                '/api/v1/secrets',
                { labelSelector: LEGACY_CONFIG_LABELS.sourceConfigMap, resourceVersion: '100' },
                expect.any(Function),
                expect.any(Function)
            );
        });
    });

    describe('_syncSecrets', () => {
        async function triggerConfigMapEvent(type: string, configMap: k8s.V1ConfigMap, labelSelector = CONFIG_LABELS.targetSecret) {
            const configMapCallback = mockWatch.mock.calls.find(
                call => call[0] === '/api/v1/configmaps' && call[1].labelSelector === labelSelector
            )?.[2];
            if (configMapCallback) {
                configMapCallback(type, configMap);
            }
        }

        async function triggerSecretEvent(type: string, secret: k8s.V1Secret, labelSelector = CONFIG_LABELS.sourceConfigMap) {
            const secretCallback = mockWatch.mock.calls.find(call => call[0] === '/api/v1/secrets' && call[1].labelSelector === labelSelector)?.[2];
            if (secretCallback) {
                secretCallback(type, secret);
            }
        }

        async function startManagerAndWait() {
            const startPromise = manager.start();
            await jest.advanceTimersByTimeAsync(5000);
            await startPromise;
        }

        it('should create a secret when a new configmap is added', async () => {
            mockParseEnvContent.mockResolvedValue({ KEY: 'value', OTHER: 'data' });
            mockCoreV1Api.createNamespacedSecret.mockResolvedValue({
                metadata: { name: 'my-secret', namespace: 'default' },
                data: { KEY: Buffer.from('value').toString('base64') }
            });

            await startManagerAndWait();

            const configMap = createConfigMap('my-config', 'default', 'my-secret', '12345');
            await triggerConfigMapEvent('ADDED', configMap);

            // Allow sync to complete
            await jest.advanceTimersByTimeAsync(0);
            await Promise.resolve();

            expect(mockCoreV1Api.createNamespacedSecret).toHaveBeenCalledWith({
                namespace: 'default',
                body: expect.objectContaining({
                    metadata: expect.objectContaining({
                        name: 'my-secret',
                        namespace: 'default',
                        labels: {
                            [CONFIG_LABELS.sourceConfigMap]: 'my-config',
                            [CONFIG_LABELS.sourceConfigMapVersion]: '12345',
                            [LEGACY_CONFIG_LABELS.sourceConfigMap]: 'my-config',
                            [LEGACY_CONFIG_LABELS.sourceConfigMapVersion]: '12345'
                        }
                    }),
                    data: {
                        KEY: Buffer.from('value').toString('base64'),
                        OTHER: Buffer.from('data').toString('base64')
                    }
                })
            });
        });

        it('should delete a secret when its source configmap is deleted', async () => {
            mockCoreV1Api.deleteNamespacedSecret.mockResolvedValue({});

            await startManagerAndWait();

            const secret = createSecret('my-secret', 'default', 'my-config', '12345');
            await triggerSecretEvent('ADDED', secret);

            // Now simulate configmap being deleted (add then delete)
            const configMap = createConfigMap('my-config', 'default', 'my-secret', '12345');
            await triggerConfigMapEvent('ADDED', configMap);
            await triggerConfigMapEvent('DELETED', configMap);

            // Allow sync to complete
            await jest.advanceTimersByTimeAsync(0);
            await Promise.resolve();

            expect(mockCoreV1Api.deleteNamespacedSecret).toHaveBeenCalledWith({
                name: 'my-secret',
                namespace: 'default'
            });
        });

        it('should delete an orphaned secret without logging errors', async () => {
            mockCoreV1Api.deleteNamespacedSecret.mockResolvedValue({});

            await startManagerAndWait();

            const orphanSecret = createSecret('orphan-secret', 'default', 'missing-config', '1');
            await triggerSecretEvent('ADDED', orphanSecret);

            await jest.advanceTimersByTimeAsync(0);
            await Promise.resolve();

            expect(mockCoreV1Api.deleteNamespacedSecret).toHaveBeenCalledWith({
                name: 'orphan-secret',
                namespace: 'default'
            });
            expect(mockLogger.error).not.toHaveBeenCalled();
        });

        it('should update a secret when configmap resourceVersion changes', async () => {
            mockParseEnvContent.mockResolvedValue({ UPDATED_KEY: 'new-value' });
            mockCoreV1Api.replaceNamespacedSecret.mockResolvedValue({
                metadata: { name: 'my-secret', namespace: 'default' },
                data: { UPDATED_KEY: Buffer.from('new-value').toString('base64') }
            });

            await startManagerAndWait();

            // First add the configmap with old version
            const configMap = createConfigMap('my-config', 'default', 'my-secret', '12345');
            await triggerConfigMapEvent('ADDED', configMap);

            // Add the secret that was created from the configmap
            const secret = createSecret('my-secret', 'default', 'my-config', '12345');
            await triggerSecretEvent('ADDED', secret);

            // Let any sync from events complete
            await jest.advanceTimersByTimeAsync(100);

            // Clear mocks to check only the update
            mockCoreV1Api.createNamespacedSecret.mockClear();
            mockCoreV1Api.replaceNamespacedSecret.mockClear();

            // Now update the configmap with a newer version
            const updatedConfigMap = createConfigMap('my-config', 'default', 'my-secret', '67890');
            await triggerConfigMapEvent('MODIFIED', updatedConfigMap);

            // Allow sync to complete
            await jest.advanceTimersByTimeAsync(100);

            expect(mockCoreV1Api.replaceNamespacedSecret).toHaveBeenCalledWith({
                name: 'my-secret',
                namespace: 'default',
                body: expect.objectContaining({
                    metadata: expect.objectContaining({
                        name: 'my-secret',
                        labels: expect.objectContaining({
                            [CONFIG_LABELS.sourceConfigMapVersion]: '67890',
                            [LEGACY_CONFIG_LABELS.sourceConfigMapVersion]: '67890'
                        })
                    })
                })
            });
        });

        it('should not create a secret when configmap and secret versions match', async () => {
            await startManagerAndWait();

            // Add configmap first
            const configMap = createConfigMap('my-config', 'default', 'my-secret', '12345');
            await triggerConfigMapEvent('ADDED', configMap);

            // Add secret with matching version
            const secret = createSecret('my-secret', 'default', 'my-config', '12345');
            await triggerSecretEvent('ADDED', secret);

            // Let any sync from events complete
            await jest.advanceTimersByTimeAsync(100);

            // Clear mocks to ensure we only check calls after this point
            mockCoreV1Api.createNamespacedSecret.mockClear();
            mockCoreV1Api.replaceNamespacedSecret.mockClear();

            // Trigger another sync by modifying the configmap with same version (no real change)
            await triggerConfigMapEvent('MODIFIED', configMap);

            // Allow sync to complete
            await jest.advanceTimersByTimeAsync(100);

            expect(mockCoreV1Api.createNamespacedSecret).not.toHaveBeenCalled();
            expect(mockCoreV1Api.replaceNamespacedSecret).not.toHaveBeenCalled();
        });

        it('should not update a secret when legacy source configmap version label matches', async () => {
            mockParseEnvContent.mockResolvedValue({ KEY: 'value' });
            mockCoreV1Api.createNamespacedSecret.mockResolvedValue({
                metadata: { name: 'my-secret', namespace: 'default' },
                data: { KEY: Buffer.from('value').toString('base64') }
            });

            await startManagerAndWait();

            const configMap = createConfigMap('my-config', 'default', 'my-secret', '12345');
            await triggerConfigMapEvent('ADDED', configMap);

            const secret: k8s.V1Secret = {
                apiVersion: 'v1',
                kind: 'Secret',
                metadata: {
                    name: 'my-secret',
                    namespace: 'default',
                    labels: {
                        [LEGACY_CONFIG_LABELS.sourceConfigMap]: 'my-config',
                        [LEGACY_CONFIG_LABELS.sourceConfigMapVersion]: '12345'
                    }
                },
                type: 'Opaque',
                data: {}
            };
            await triggerSecretEvent('ADDED', secret, LEGACY_CONFIG_LABELS.sourceConfigMap);

            await jest.advanceTimersByTimeAsync(100);

            mockCoreV1Api.createNamespacedSecret.mockClear();
            mockCoreV1Api.replaceNamespacedSecret.mockClear();

            await triggerConfigMapEvent('MODIFIED', configMap);
            await jest.advanceTimersByTimeAsync(100);

            expect(mockCoreV1Api.createNamespacedSecret).not.toHaveBeenCalled();
            expect(mockCoreV1Api.replaceNamespacedSecret).not.toHaveBeenCalled();
        });

        it('should clean up cache when both secret and configmap are undefined', async () => {
            await startManagerAndWait();

            // Add then delete configmap (no secret ever existed)
            const configMap = createConfigMap('my-config', 'default', 'orphan-secret', '12345');
            await triggerConfigMapEvent('ADDED', configMap);
            await triggerConfigMapEvent('DELETED', configMap);

            // Allow sync to complete
            await jest.advanceTimersByTimeAsync(0);
            await Promise.resolve();

            // Since there's no secret, nothing should be deleted from k8s
            expect(mockCoreV1Api.deleteNamespacedSecret).not.toHaveBeenCalled();
        });

        it('should clear cache when secret deletion fails with 404', async () => {
            mockCoreV1Api.deleteNamespacedSecret.mockRejectedValue({ code: 404 });

            await startManagerAndWait();

            const orphanSecret = createSecret('gone-secret', 'default', 'missing-config', '1');
            await triggerSecretEvent('ADDED', orphanSecret);

            await jest.advanceTimersByTimeAsync(0);
            await Promise.resolve();

            expect(mockCoreV1Api.deleteNamespacedSecret).toHaveBeenCalledWith({
                name: 'gone-secret',
                namespace: 'default'
            });
            // Should not log an error for 404
            expect(mockLogger.error).not.toHaveBeenCalled();

            // Trigger another sync - the cache entry should be gone so no more delete attempts
            mockCoreV1Api.deleteNamespacedSecret.mockClear();
            manager.syncSecrets();
            await jest.advanceTimersByTimeAsync(0);
            await Promise.resolve();

            expect(mockCoreV1Api.deleteNamespacedSecret).not.toHaveBeenCalled();
        });
    });

    describe('createSecretForConfigMap', () => {
        async function startManagerAndWait() {
            const startPromise = manager.start();
            await jest.advanceTimersByTimeAsync(5000);
            await startPromise;
        }

        async function triggerConfigMapEvent(type: string, configMap: k8s.V1ConfigMap, labelSelector = CONFIG_LABELS.targetSecret) {
            const configMapCallback = mockWatch.mock.calls.find(
                call => call[0] === '/api/v1/configmaps' && call[1].labelSelector === labelSelector
            )?.[2];
            if (configMapCallback) {
                configMapCallback(type, configMap);
            }
        }

        it('should use custom source key from label', async () => {
            mockParseEnvContent.mockResolvedValue({ KEY: 'value' });
            mockCoreV1Api.createNamespacedSecret.mockResolvedValue({
                metadata: { name: 'my-secret', namespace: 'default' }
            });

            await startManagerAndWait();

            const configMap: k8s.V1ConfigMap = {
                apiVersion: 'v1',
                kind: 'ConfigMap',
                metadata: {
                    name: 'my-config',
                    namespace: 'default',
                    resourceVersion: '12345',
                    labels: {
                        [CONFIG_LABELS.targetSecret]: 'my-secret',
                        [CONFIG_LABELS.sourceKey]: 'custom.env'
                    }
                },
                data: {
                    'custom.env': 'CUSTOM_KEY=custom_value'
                }
            };

            await triggerConfigMapEvent('ADDED', configMap);
            await jest.advanceTimersByTimeAsync(0);
            await Promise.resolve();

            expect(mockParseEnvContent).toHaveBeenCalledWith('CUSTOM_KEY=custom_value', undefined);
        });

        it('should use legacy source configmap labels', async () => {
            mockParseEnvContent.mockResolvedValue({ KEY: 'value' });
            mockCoreV1Api.createNamespacedSecret.mockResolvedValue({
                metadata: { name: 'my-secret', namespace: 'default' }
            });

            await startManagerAndWait();

            const configMap: k8s.V1ConfigMap = {
                apiVersion: 'v1',
                kind: 'ConfigMap',
                metadata: {
                    name: 'my-config',
                    namespace: 'default',
                    resourceVersion: '12345',
                    labels: {
                        [LEGACY_CONFIG_LABELS.targetSecret]: 'my-secret',
                        [LEGACY_CONFIG_LABELS.sourceKey]: 'legacy.env'
                    }
                },
                data: {
                    'legacy.env': 'LEGACY_KEY=legacy_value'
                }
            };

            await triggerConfigMapEvent('ADDED', configMap, LEGACY_CONFIG_LABELS.targetSecret);
            await jest.advanceTimersByTimeAsync(0);
            await Promise.resolve();

            expect(mockParseEnvContent).toHaveBeenCalledWith('LEGACY_KEY=legacy_value', undefined);
            expect(mockCoreV1Api.createNamespacedSecret).toHaveBeenCalledWith({
                namespace: 'default',
                body: expect.objectContaining({
                    metadata: expect.objectContaining({
                        name: 'my-secret',
                        labels: expect.objectContaining({
                            [CONFIG_LABELS.sourceConfigMap]: 'my-config',
                            [CONFIG_LABELS.sourceConfigMapVersion]: '12345',
                            [LEGACY_CONFIG_LABELS.sourceConfigMap]: 'my-config',
                            [LEGACY_CONFIG_LABELS.sourceConfigMapVersion]: '12345'
                        })
                    })
                })
            });
        });

        it('should throw error when source key is not found in configmap', async () => {
            await startManagerAndWait();

            const configMap: k8s.V1ConfigMap = {
                apiVersion: 'v1',
                kind: 'ConfigMap',
                metadata: {
                    name: 'my-config',
                    namespace: 'default',
                    resourceVersion: '12345',
                    labels: {
                        [CONFIG_LABELS.targetSecret]: 'my-secret',
                        [CONFIG_LABELS.sourceKey]: 'nonexistent.env'
                    }
                },
                data: {
                    '.env': 'KEY=value'
                }
            };

            await triggerConfigMapEvent('ADDED', configMap);
            await jest.advanceTimersByTimeAsync(0);
            await Promise.resolve();

            // The error should be caught and logged, but not thrown
            expect(mockCoreV1Api.createNamespacedSecret).not.toHaveBeenCalled();
        });

        it('should retrieve decryption secret from referenced secret', async () => {
            mockParseEnvContent.mockResolvedValue({ KEY: 'decrypted_value' });
            mockCoreV1Api.readNamespacedSecret.mockResolvedValue({
                data: {
                    CONFIG_DECRYPTION_SECRET: Buffer.from('my-secret-key').toString('base64')
                }
            });
            mockCoreV1Api.createNamespacedSecret.mockResolvedValue({
                metadata: { name: 'my-secret', namespace: 'default' }
            });

            await startManagerAndWait();

            const configMap: k8s.V1ConfigMap = {
                apiVersion: 'v1',
                kind: 'ConfigMap',
                metadata: {
                    name: 'my-config',
                    namespace: 'default',
                    resourceVersion: '12345',
                    labels: {
                        [CONFIG_LABELS.targetSecret]: 'my-secret',
                        [CONFIG_LABELS.decryptionSecret]: 'key-secret'
                    }
                },
                data: {
                    '.env': 'ENCRYPTED_KEY=encrypted_value'
                }
            };

            await triggerConfigMapEvent('ADDED', configMap);
            await jest.advanceTimersByTimeAsync(0);
            await Promise.resolve();

            expect(mockCoreV1Api.readNamespacedSecret).toHaveBeenCalledWith({
                name: 'key-secret',
                namespace: 'default'
            });
            expect(mockParseEnvContent).toHaveBeenCalledWith('ENCRYPTED_KEY=encrypted_value', 'my-secret-key');
        });

        it('should fall back to the legacy decryption key name', async () => {
            mockParseEnvContent.mockResolvedValue({ KEY: 'decrypted_value' });
            mockCoreV1Api.readNamespacedSecret.mockResolvedValue({
                data: {
                    CONFIG_DECRYPTION_KEY: Buffer.from('legacy-secret-key').toString('base64')
                }
            });
            mockCoreV1Api.createNamespacedSecret.mockResolvedValue({
                metadata: { name: 'my-secret', namespace: 'default' }
            });

            await startManagerAndWait();

            const configMap: k8s.V1ConfigMap = {
                apiVersion: 'v1',
                kind: 'ConfigMap',
                metadata: {
                    name: 'my-config',
                    namespace: 'default',
                    resourceVersion: '12345',
                    labels: {
                        [CONFIG_LABELS.targetSecret]: 'my-secret',
                        [CONFIG_LABELS.decryptionSecret]: 'key-secret'
                    }
                },
                data: {
                    '.env': 'ENCRYPTED_KEY=encrypted_value'
                }
            };

            await triggerConfigMapEvent('ADDED', configMap);
            await jest.advanceTimersByTimeAsync(0);
            await Promise.resolve();

            expect(mockParseEnvContent).toHaveBeenCalledWith('ENCRYPTED_KEY=encrypted_value', 'legacy-secret-key');
        });

        it('should prefer the new decryption secret name when both default names are present', async () => {
            mockParseEnvContent.mockResolvedValue({ KEY: 'decrypted_value' });
            mockCoreV1Api.readNamespacedSecret.mockResolvedValue({
                data: {
                    CONFIG_DECRYPTION_SECRET: Buffer.from('preferred-secret-key').toString('base64'),
                    CONFIG_DECRYPTION_KEY: Buffer.from('legacy-secret-key').toString('base64')
                }
            });
            mockCoreV1Api.createNamespacedSecret.mockResolvedValue({
                metadata: { name: 'my-secret', namespace: 'default' }
            });

            await startManagerAndWait();

            const configMap: k8s.V1ConfigMap = {
                apiVersion: 'v1',
                kind: 'ConfigMap',
                metadata: {
                    name: 'my-config',
                    namespace: 'default',
                    resourceVersion: '12345',
                    labels: {
                        [CONFIG_LABELS.targetSecret]: 'my-secret',
                        [CONFIG_LABELS.decryptionSecret]: 'key-secret'
                    }
                },
                data: {
                    '.env': 'ENCRYPTED_KEY=encrypted_value'
                }
            };

            await triggerConfigMapEvent('ADDED', configMap);
            await jest.advanceTimersByTimeAsync(0);
            await Promise.resolve();

            expect(mockParseEnvContent).toHaveBeenCalledWith('ENCRYPTED_KEY=encrypted_value', 'preferred-secret-key');
        });

        it('should use custom decryption key name from label', async () => {
            mockParseEnvContent.mockResolvedValue({ KEY: 'decrypted_value' });
            mockCoreV1Api.readNamespacedSecret.mockResolvedValue({
                data: {
                    CUSTOM_KEY_NAME: Buffer.from('custom-key-value').toString('base64')
                }
            });
            mockCoreV1Api.createNamespacedSecret.mockResolvedValue({
                metadata: { name: 'my-secret', namespace: 'default' }
            });

            await startManagerAndWait();

            const configMap: k8s.V1ConfigMap = {
                apiVersion: 'v1',
                kind: 'ConfigMap',
                metadata: {
                    name: 'my-config',
                    namespace: 'default',
                    resourceVersion: '12345',
                    labels: {
                        [CONFIG_LABELS.targetSecret]: 'my-secret',
                        [CONFIG_LABELS.decryptionSecret]: 'key-secret',
                        [CONFIG_LABELS.decryptionSecretKey]: 'CUSTOM_KEY_NAME'
                    }
                },
                data: {
                    '.env': 'ENCRYPTED_KEY=encrypted_value'
                }
            };

            await triggerConfigMapEvent('ADDED', configMap);
            await jest.advanceTimersByTimeAsync(0);
            await Promise.resolve();

            expect(mockCoreV1Api.readNamespacedSecret).toHaveBeenCalledWith({
                name: 'key-secret',
                namespace: 'default'
            });
            expect(mockParseEnvContent).toHaveBeenCalledWith('ENCRYPTED_KEY=encrypted_value', 'custom-key-value');
        });
    });

    describe('watch error handling', () => {
        it('should retry configmap watch on error', async () => {
            const startPromise = manager.start();
            await jest.advanceTimersByTimeAsync(5000);
            await startPromise;

            // Get the error callback for configmaps
            const errorCallback = mockWatch.mock.calls.find(call => call[0] === '/api/v1/configmaps')?.[3];
            expect(errorCallback).toBeDefined();

            // Simulate an error
            errorCallback(new Error('Watch failed'));

            // Fast-forward through the retry timeout
            await jest.advanceTimersByTimeAsync(1000);

            // Should have retried the watch
            expect(mockWatch).toHaveBeenCalledTimes(5); // 4 initial + 1 retry
        });

        it('should retry secret watch on error', async () => {
            const startPromise = manager.start();
            await jest.advanceTimersByTimeAsync(5000);
            await startPromise;

            // Get the error callback for secrets
            const errorCallback = mockWatch.mock.calls.find(call => call[0] === '/api/v1/secrets')?.[3];
            expect(errorCallback).toBeDefined();

            // Simulate an error
            errorCallback(new Error('Watch failed'));

            // Fast-forward through the retry timeout
            await jest.advanceTimersByTimeAsync(1000);

            // Should have retried the watch
            expect(mockWatch).toHaveBeenCalledTimes(5); // 4 initial + 1 retry
        });
    });

    describe('graceful shutdown', () => {
        it('should clear the sync interval and abort watch connections', async () => {
            const startPromise = manager.start();
            await jest.advanceTimersByTimeAsync(5000);
            await startPromise;

            const abortControllers = await Promise.all(mockWatch.mock.results.map(result => result.value));

            manager.stop();

            for (const abortController of abortControllers) {
                expect(abortController.abort).toHaveBeenCalled();
            }
        });

        it('should not retry watches after stop is called', async () => {
            const startPromise = manager.start();
            await jest.advanceTimersByTimeAsync(5000);
            await startPromise;

            const configMapErrorCb = mockWatch.mock.calls.find(call => call[0] === '/api/v1/configmaps')?.[3];
            const secretErrorCb = mockWatch.mock.calls.find(call => call[0] === '/api/v1/secrets')?.[3];

            manager.stop();

            // Trigger errors after stop
            configMapErrorCb(new Error('Watch closed'));
            secretErrorCb(new Error('Watch closed'));

            // Advance past any potential retry delay
            await jest.advanceTimersByTimeAsync(60_000);

            // Should still only have the 4 initial watch calls - no retries
            expect(mockWatch).toHaveBeenCalledTimes(4);
        });

        it('should not reconcile after stop() during the startup delay', async () => {
            mockParseEnvContent.mockResolvedValue({ KEY: 'value' });
            mockCoreV1Api.listConfigMapForAllNamespaces.mockResolvedValue({
                items: [createConfigMap('cm1', 'default', 'sec1', '1')],
                metadata: { resourceVersion: '100' }
            });

            const startPromise = manager.start();
            await jest.advanceTimersByTimeAsync(0);

            manager.stop();
            await jest.advanceTimersByTimeAsync(35_000);
            await startPromise;

            // start() used to set isReady and create a new interval after stop(), which
            // reconciled the listed ConfigMap at 5s and every 30s thereafter.
            expect(mockCoreV1Api.createNamespacedSecret).not.toHaveBeenCalled();
        });

        it('should not list or reconcile from a retry queued before stop()', async () => {
            const startPromise = manager.start();
            await jest.advanceTimersByTimeAsync(5000);
            await startPromise;

            const configMapErrorCb = mockWatch.mock.calls.find(call => call[0] === '/api/v1/configmaps')?.[3];
            configMapErrorCb(new Error('Watch closed'));
            const listCallsBeforeStop = mockCoreV1Api.listConfigMapForAllNamespaces.mock.calls.length;

            manager.stop();
            await jest.advanceTimersByTimeAsync(1000);

            expect(mockCoreV1Api.listConfigMapForAllNamespaces).toHaveBeenCalledTimes(listCallsBeforeStop);
        });
    });

    describe('exponential backoff', () => {
        it('should use exponential backoff for consecutive watch failures', async () => {
            const startPromise = manager.start();
            await jest.advanceTimersByTimeAsync(5000);
            await startPromise;

            // Initial call count
            expect(mockWatch).toHaveBeenCalledTimes(4);

            // First failure - should retry after 1s (initial delay)
            const errorCallback1 = mockWatch.mock.calls.find(call => call[0] === '/api/v1/configmaps')?.[3];
            errorCallback1(new Error('fail'));

            await jest.advanceTimersByTimeAsync(1000);
            expect(mockWatch).toHaveBeenCalledTimes(5);

            // Second failure - should retry after 2s
            const errorCallback2 = mockWatch.mock.calls[4][3];
            errorCallback2(new Error('fail'));

            // Not yet at 2s
            await jest.advanceTimersByTimeAsync(1000);
            expect(mockWatch).toHaveBeenCalledTimes(5);

            // Now at 2s
            await jest.advanceTimersByTimeAsync(1000);
            expect(mockWatch).toHaveBeenCalledTimes(6);

            // Third failure - should retry after 4s
            const errorCallback3 = mockWatch.mock.calls[5][3];
            errorCallback3(new Error('fail'));

            await jest.advanceTimersByTimeAsync(3999);
            expect(mockWatch).toHaveBeenCalledTimes(6);

            await jest.advanceTimersByTimeAsync(1);
            expect(mockWatch).toHaveBeenCalledTimes(7);
        });
    });

    describe('list and watch', () => {
        it('should start the watch from the list resourceVersion', async () => {
            mockCoreV1Api.listConfigMapForAllNamespaces.mockResolvedValue({
                items: [],
                metadata: { resourceVersion: '742' }
            });

            const startPromise = manager.start();
            await jest.advanceTimersByTimeAsync(5000);
            await startPromise;

            const configMapWatch = mockWatch.mock.calls.find(call => call[0] === '/api/v1/configmaps');
            expect(configMapWatch[1]).toEqual({
                labelSelector: CONFIG_LABELS.targetSecret,
                resourceVersion: '742'
            });
        });

        it('should seed the cache from the list', async () => {
            mockParseEnvContent.mockResolvedValue({ KEY: 'value' });
            mockCoreV1Api.listConfigMapForAllNamespaces.mockResolvedValue({
                items: [createConfigMap('cm1', 'default', 'sec1', '1')],
                metadata: { resourceVersion: '742' }
            });
            mockCoreV1Api.createNamespacedSecret.mockResolvedValue({ metadata: { name: 'sec1' } });

            const startPromise = manager.start();
            await jest.advanceTimersByTimeAsync(5000);
            await startPromise;
            await jest.advanceTimersByTimeAsync(0);

            expect(mockCoreV1Api.createNamespacedSecret).toHaveBeenCalled();
        });

        it('should re-list on reconnect, so an expired resourceVersion cannot wedge the watch', async () => {
            const startPromise = manager.start();
            await jest.advanceTimersByTimeAsync(5000);
            await startPromise;

            expect(mockCoreV1Api.listConfigMapForAllNamespaces).toHaveBeenCalledTimes(2); // one per selector

            // A 410 arrives as an ERROR event, then the stream closes with a null error.
            const configMapCall = mockWatch.mock.calls.find(call => call[0] === '/api/v1/configmaps');
            configMapCall[2]('ERROR', { kind: 'Status', reason: 'Expired', code: 410, metadata: {} });
            configMapCall[3](null);

            mockCoreV1Api.listConfigMapForAllNamespaces.mockResolvedValue({
                items: [],
                metadata: { resourceVersion: '9000' }
            });

            await jest.advanceTimersByTimeAsync(1000);

            // The reconnect must go through a fresh LIST and use its version — never the
            // expired one, which is what looped forever before.
            expect(mockCoreV1Api.listConfigMapForAllNamespaces).toHaveBeenCalledTimes(3);
            expect(mockWatch.mock.calls[4][1]).toEqual({
                labelSelector: CONFIG_LABELS.targetSecret,
                resourceVersion: '9000'
            });
        });

        it('should ignore BOOKMARK events', async () => {
            const startPromise = manager.start();
            await jest.advanceTimersByTimeAsync(5000);
            await startPromise;

            const configMapEventCb = mockWatch.mock.calls.find(call => call[0] === '/api/v1/configmaps')?.[2];
            configMapEventCb('BOOKMARK', { metadata: { resourceVersion: '900' } });

            await jest.advanceTimersByTimeAsync(30_000);

            // A bookmark is not a resource: it must not reach the cache, and it must not be
            // mistaken for a ConfigMap that declares no target secret.
            expect(Object.keys(manager.cache)).toHaveLength(0);
            expect(mockCoreV1Api.createNamespacedSecret).not.toHaveBeenCalled();
        });

        it('should retry with backoff when the list itself fails', async () => {
            mockCoreV1Api.listConfigMapForAllNamespaces.mockRejectedValue(new Error('api down'));

            const startPromise = manager.start();
            await jest.advanceTimersByTimeAsync(5000);
            await startPromise;

            // A failed LIST must not open a watch — there is no safe resourceVersion to
            // start one from.
            expect(mockWatch.mock.calls.filter(call => call[0] === '/api/v1/configmaps')).toHaveLength(0);

            // Retries are queued and keep firing (the 5s startup wait already let some
            // through, so assert on progress rather than an exact count).
            const afterStartup = mockCoreV1Api.listConfigMapForAllNamespaces.mock.calls.length;
            expect(afterStartup).toBeGreaterThan(2);

            await jest.advanceTimersByTimeAsync(60_000);
            expect(mockCoreV1Api.listConfigMapForAllNamespaces.mock.calls.length).toBeGreaterThan(afterStartup);

            // Secrets listed fine, so their watches are unaffected by the ConfigMap failure.
            expect(mockWatch.mock.calls.filter(call => call[0] === '/api/v1/secrets')).toHaveLength(2);
        });

        it('should not reset the retry backoff on ERROR events', async () => {
            const startPromise = manager.start();
            await jest.advanceTimersByTimeAsync(5000);
            await startPromise;

            const status = { kind: 'Status', reason: 'Expired', code: 410, metadata: {} };
            const failWatch = (callIndex: number) => {
                mockWatch.mock.calls[callIndex][2]('ERROR', status);
                mockWatch.mock.calls[callIndex][3](null);
            };

            // Each reconnect immediately 410s. If ERROR reset the backoff — or if a merely
            // successful LIST did — the delay would stay at 1s and hot loop.
            const configMapCallIndex = mockWatch.mock.calls.findIndex(call => call[0] === '/api/v1/configmaps');
            failWatch(configMapCallIndex);
            await jest.advanceTimersByTimeAsync(1000);
            expect(mockWatch).toHaveBeenCalledTimes(5);

            failWatch(4);
            await jest.advanceTimersByTimeAsync(1000);
            expect(mockWatch).toHaveBeenCalledTimes(5);
            await jest.advanceTimersByTimeAsync(1000);
            expect(mockWatch).toHaveBeenCalledTimes(6);
        });

        it('should reset the retry backoff once a watch has stayed up', async () => {
            const startPromise = manager.start();
            await jest.advanceTimersByTimeAsync(5000);
            await startPromise;

            const configMapCallIndex = mockWatch.mock.calls.findIndex(call => call[0] === '/api/v1/configmaps');

            // Fail twice to push the delay to 4s.
            mockWatch.mock.calls[configMapCallIndex][3](null);
            await jest.advanceTimersByTimeAsync(1000);
            mockWatch.mock.calls[4][3](null);
            await jest.advanceTimersByTimeAsync(2000);
            expect(mockWatch).toHaveBeenCalledTimes(6);

            // This one survives past the healthy threshold before dying, so the next
            // reconnect should be back to the 1s initial delay.
            await jest.advanceTimersByTimeAsync(10_000);
            mockWatch.mock.calls[5][3](null);
            await jest.advanceTimersByTimeAsync(1000);
            expect(mockWatch).toHaveBeenCalledTimes(7);
        });
    });

    describe('shutdown races', () => {
        it('should not open a watch when stop() lands while the list is in flight', async () => {
            let resolveList: (value: unknown) => void = () => {};
            mockCoreV1Api.listConfigMapForAllNamespaces.mockReturnValue(
                new Promise(resolve => {
                    resolveList = resolve;
                })
            );

            const startPromise = manager.start();
            await Promise.resolve();

            manager.stop();
            resolveList({ items: [], metadata: { resourceVersion: '1' } });

            await jest.advanceTimersByTimeAsync(5000);
            await startPromise;

            // A watch opened now would outlive the abort controllers stop() already drained.
            expect(mockWatch.mock.calls.filter(call => call[0] === '/api/v1/configmaps')).toHaveLength(0);
        });
    });

    describe('pruning entries that vanished while disconnected', () => {
        it('should delete a secret whose configmap disappeared during the outage', async () => {
            // Selector-aware: cm1 only carries the modern label, so only that selector
            // lists it. A mock that returned it for both would leave the legacy selector
            // still claiming the key, and the union rule would (correctly) refuse to prune.
            const configMap = createConfigMap('cm1', 'default', 'sec1', '1');
            mockCoreV1Api.listConfigMapForAllNamespaces.mockImplementation(({ labelSelector }: { labelSelector: string }) =>
                Promise.resolve({
                    items: labelSelector === CONFIG_LABELS.targetSecret ? [configMap] : [],
                    metadata: { resourceVersion: '1' }
                })
            );
            mockCoreV1Api.listSecretForAllNamespaces.mockResolvedValue({
                items: [createSecret('sec1', 'default', 'cm1', '1')],
                metadata: { resourceVersion: '1' }
            });
            mockCoreV1Api.deleteNamespacedSecret.mockResolvedValue({});

            const startPromise = manager.start();
            await jest.advanceTimersByTimeAsync(5000);
            await startPromise;
            await jest.advanceTimersByTimeAsync(0);

            // Versions match, so nothing was written.
            expect(mockCoreV1Api.deleteNamespacedSecret).not.toHaveBeenCalled();

            // The ConfigMap is deleted while the watch is down, so no DELETED event ever
            // arrives — only the next LIST can notice.
            mockCoreV1Api.listConfigMapForAllNamespaces.mockImplementation(() => Promise.resolve({ items: [], metadata: { resourceVersion: '2' } }));

            const configMapCall = mockWatch.mock.calls.find(
                call => call[0] === '/api/v1/configmaps' && call[1].labelSelector === CONFIG_LABELS.targetSecret
            );
            configMapCall[3](null);
            await jest.advanceTimersByTimeAsync(1000);

            expect(mockCoreV1Api.deleteNamespacedSecret).toHaveBeenCalledWith({
                name: 'sec1',
                namespace: 'default'
            });
        });

        it('should not prune an entry still listed by the other label selector', async () => {
            // Only the legacy selector matches this ConfigMap — the modern selector's list
            // comes back empty, and must not be read as "it is gone".
            const legacyConfigMap: k8s.V1ConfigMap = {
                metadata: {
                    name: 'cm1',
                    namespace: 'default',
                    resourceVersion: '1',
                    labels: { [LEGACY_CONFIG_LABELS.targetSecret]: 'sec1' }
                },
                data: { '.env': 'KEY=value' }
            };
            mockCoreV1Api.listConfigMapForAllNamespaces.mockImplementation(({ labelSelector }: { labelSelector: string }) =>
                Promise.resolve({
                    items: labelSelector === LEGACY_CONFIG_LABELS.targetSecret ? [legacyConfigMap] : [],
                    metadata: { resourceVersion: '1' }
                })
            );
            mockCoreV1Api.listSecretForAllNamespaces.mockResolvedValue({
                items: [createSecret('sec1', 'default', 'cm1', '1')],
                metadata: { resourceVersion: '1' }
            });

            const startPromise = manager.start();
            await jest.advanceTimersByTimeAsync(5000);
            await startPromise;
            await jest.advanceTimersByTimeAsync(0);

            expect(manager.cache['default/sec1'].configMap).toBeDefined();

            // Now force the *modern* selector to re-list, after the legacy selector has
            // already claimed the key. Its list is empty, but that only means "no ConfigMap
            // carries the modern label" — not "the ConfigMap is gone". Pruning on one
            // selector's view alone would delete a live Secret here.
            const modernCall = mockWatch.mock.calls.find(
                call => call[0] === '/api/v1/configmaps' && call[1].labelSelector === CONFIG_LABELS.targetSecret
            );
            modernCall[3](null);
            await jest.advanceTimersByTimeAsync(1000);

            expect(manager.cache['default/sec1'].configMap).toBeDefined();
            expect(mockCoreV1Api.deleteNamespacedSecret).not.toHaveBeenCalled();
        });
    });

    describe('overlapping label selectors', () => {
        it('should retain resources claimed by the other selector after a DELETED event', async () => {
            const startPromise = manager.start();
            await jest.advanceTimersByTimeAsync(5000);
            await startPromise;

            const configMap: k8s.V1ConfigMap = {
                metadata: {
                    name: 'cm1',
                    namespace: 'default',
                    resourceVersion: '1',
                    labels: {
                        [CONFIG_LABELS.targetSecret]: 'sec1',
                        [LEGACY_CONFIG_LABELS.targetSecret]: 'sec1'
                    }
                },
                data: { '.env': 'KEY=value' }
            };
            const secret: k8s.V1Secret = {
                metadata: {
                    name: 'sec1',
                    namespace: 'default',
                    labels: {
                        [CONFIG_LABELS.sourceConfigMap]: 'cm1',
                        [CONFIG_LABELS.sourceConfigMapVersion]: '1',
                        [LEGACY_CONFIG_LABELS.sourceConfigMap]: 'cm1',
                        [LEGACY_CONFIG_LABELS.sourceConfigMapVersion]: '1'
                    }
                }
            };
            const cacheKey = 'default/sec1';
            manager.cache[cacheKey] = { configMap, secret };
            manager.configMapKeysBySelector.set(CONFIG_LABELS.targetSecret, new Set([cacheKey]));
            manager.configMapKeysBySelector.set(LEGACY_CONFIG_LABELS.targetSecret, new Set([cacheKey]));
            manager.secretKeysBySelector.set(CONFIG_LABELS.sourceConfigMap, new Set([cacheKey]));
            manager.secretKeysBySelector.set(LEGACY_CONFIG_LABELS.sourceConfigMap, new Set([cacheKey]));

            const configMapEventCb = mockWatch.mock.calls.find(
                call => call[0] === '/api/v1/configmaps' && call[1].labelSelector === CONFIG_LABELS.targetSecret
            )?.[2];
            const secretEventCb = mockWatch.mock.calls.find(
                call => call[0] === '/api/v1/secrets' && call[1].labelSelector === CONFIG_LABELS.sourceConfigMap
            )?.[2];

            // A label migration can remove a resource from one watch before the other
            // stream delivers its corresponding add/modify event.
            configMapEventCb('DELETED', {
                ...configMap,
                metadata: { ...configMap.metadata, labels: { [LEGACY_CONFIG_LABELS.targetSecret]: 'sec1' } }
            });
            secretEventCb('DELETED', secret);
            await jest.advanceTimersByTimeAsync(0);

            expect(manager.cache[cacheKey]).toEqual({ configMap, secret });
            expect(mockCoreV1Api.deleteNamespacedSecret).not.toHaveBeenCalled();
            expect(mockCoreV1Api.createNamespacedSecret).not.toHaveBeenCalled();
        });
    });

    describe('malformed watch events', () => {
        it('should not cache a Status object delivered on the Secret watch', async () => {
            const startPromise = manager.start();
            await jest.advanceTimersByTimeAsync(5000);
            await startPromise;

            const secretEventCallback = mockWatch.mock.calls.find(call => call[0] === '/api/v1/secrets')?.[2];

            // Before the fix this cached the Status under the key "undefined/undefined",
            // which _syncSecrets then read as "a secret whose ConfigMap is gone" and tried
            // to delete by an undefined name — forever. Note the `metadata: {}`: a real
            // Status carries a V1ListMeta, which is why production reached the API call and
            // failed there rather than throwing on the property access.
            secretEventCallback('ERROR', {
                kind: 'Status',
                apiVersion: 'v1',
                status: 'Failure',
                reason: 'Expired',
                code: 410,
                metadata: {}
            });

            await jest.advanceTimersByTimeAsync(30_000);

            expect(manager.cache['undefined/undefined']).toBeUndefined();
            expect(mockCoreV1Api.deleteNamespacedSecret).not.toHaveBeenCalled();
        });

        it('should ignore a Secret event with no name or namespace', async () => {
            const startPromise = manager.start();
            await jest.advanceTimersByTimeAsync(5000);
            await startPromise;

            const secretEventCallback = mockWatch.mock.calls.find(call => call[0] === '/api/v1/secrets')?.[2];
            secretEventCallback('ADDED', { metadata: { resourceVersion: '7' } });

            await jest.advanceTimersByTimeAsync(30_000);

            expect(mockCoreV1Api.deleteNamespacedSecret).not.toHaveBeenCalled();
            expect(mockLogger.error).toHaveBeenCalled();
        });

        it('should drop an unaddressable cache entry instead of retrying it forever', async () => {
            const startPromise = manager.start();
            await jest.advanceTimersByTimeAsync(5000);
            await startPromise;

            // Simulate an entry that somehow reached the cache without an addressable
            // secret; the reconcile must evict it rather than loop on a nameless delete.
            manager.cache['undefined/undefined'] = { secret: { kind: 'Status' } };

            manager.syncSecrets();
            await jest.advanceTimersByTimeAsync(0);

            expect(mockCoreV1Api.deleteNamespacedSecret).not.toHaveBeenCalled();
            expect(manager.cache['undefined/undefined']).toBeUndefined();
        });
    });

    describe('syncSecrets rate limiting', () => {
        it('should not sync before ready', async () => {
            // Call syncSecrets before manager is ready
            manager.syncSecrets();

            // No API calls should be made
            expect(mockCoreV1Api.createNamespacedSecret).not.toHaveBeenCalled();
            expect(mockCoreV1Api.deleteNamespacedSecret).not.toHaveBeenCalled();
        });

        it('should queue sync if called while sync is in progress', async () => {
            mockParseEnvContent.mockResolvedValue({ KEY: 'value' });

            // Create a delayed promise to simulate slow API call
            let resolveCreate: (value: unknown) => void;
            mockCoreV1Api.createNamespacedSecret.mockImplementation(
                () =>
                    new Promise(resolve => {
                        resolveCreate = resolve;
                    })
            );

            const startPromise = manager.start();
            await jest.advanceTimersByTimeAsync(5000);
            await startPromise;

            // Add a configmap to trigger sync
            const configMapCallback = mockWatch.mock.calls.find(call => call[0] === '/api/v1/configmaps')?.[2];
            const configMap: k8s.V1ConfigMap = {
                metadata: {
                    name: 'test-config',
                    namespace: 'default',
                    resourceVersion: '1',
                    labels: { [CONFIG_LABELS.targetSecret]: 'test-secret' }
                },
                data: { '.env': 'KEY=value' }
            };

            configMapCallback('ADDED', configMap);
            await jest.advanceTimersByTimeAsync(0);

            // Now try to sync again while first sync is in progress
            manager.syncSecrets();

            // Resolve the first create
            resolveCreate!({ metadata: { name: 'test-secret' } });
            await jest.advanceTimersByTimeAsync(0);
            await Promise.resolve();

            // The pending sync should execute after first completes
            await jest.advanceTimersByTimeAsync(0);
            await Promise.resolve();

            // createNamespacedSecret should have been called at least once
            expect(mockCoreV1Api.createNamespacedSecret).toHaveBeenCalled();
        });
    });
});
