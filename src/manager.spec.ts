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
            readNamespacedSecret: jest.fn()
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
                { labelSelector: CONFIG_LABELS.targetSecret },
                expect.any(Function),
                expect.any(Function)
            );
            expect(mockWatch).toHaveBeenCalledWith(
                '/api/v1/configmaps',
                { labelSelector: LEGACY_CONFIG_LABELS.targetSecret },
                expect.any(Function),
                expect.any(Function)
            );
            expect(mockWatch).toHaveBeenCalledWith(
                '/api/v1/secrets',
                { labelSelector: CONFIG_LABELS.sourceConfigMap },
                expect.any(Function),
                expect.any(Function)
            );
            expect(mockWatch).toHaveBeenCalledWith(
                '/api/v1/secrets',
                { labelSelector: LEGACY_CONFIG_LABELS.sourceConfigMap },
                expect.any(Function),
                expect.any(Function)
            );
        });
    });

    describe('_syncSecrets', () => {
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

    describe('resourceVersion tracking', () => {
        it('should pass resourceVersion when resuming watch', async () => {
            const startPromise = manager.start();
            await jest.advanceTimersByTimeAsync(5000);
            await startPromise;

            // Simulate a configmap event with a resourceVersion
            const eventCallback = mockWatch.mock.calls.find(call => call[0] === '/api/v1/configmaps')?.[2];
            eventCallback('ADDED', {
                metadata: {
                    name: 'cm1',
                    namespace: 'default',
                    resourceVersion: '500',
                    labels: { [CONFIG_LABELS.targetSecret]: 'sec1' }
                },
                data: {}
            });

            // Now trigger a watch error to cause reconnect
            const errorCallback = mockWatch.mock.calls.find(call => call[0] === '/api/v1/configmaps')?.[3];
            errorCallback(new Error('connection lost'));

            await jest.advanceTimersByTimeAsync(1000);

            // The reconnect should include resourceVersion
            const reconnectCall = mockWatch.mock.calls[4]; // 5th call (index 4)
            expect(reconnectCall[0]).toBe('/api/v1/configmaps');
            expect(reconnectCall[1]).toEqual({
                labelSelector: CONFIG_LABELS.targetSecret,
                resourceVersion: '500'
            });
        });

        it('should reset resourceVersion on 410 Gone error', async () => {
            const startPromise = manager.start();
            await jest.advanceTimersByTimeAsync(5000);
            await startPromise;

            // Simulate a configmap event to set resourceVersion
            const eventCallback = mockWatch.mock.calls.find(call => call[0] === '/api/v1/configmaps')?.[2];
            eventCallback('ADDED', {
                metadata: {
                    name: 'cm1',
                    namespace: 'default',
                    resourceVersion: '500',
                    labels: { [CONFIG_LABELS.targetSecret]: 'sec1' }
                },
                data: {}
            });

            // Trigger a 410 Gone error
            const errorCallback = mockWatch.mock.calls.find(call => call[0] === '/api/v1/configmaps')?.[3];
            errorCallback({ code: 410, message: 'Gone' });

            await jest.advanceTimersByTimeAsync(1000);

            // The reconnect should NOT include resourceVersion (it was reset)
            const reconnectCall = mockWatch.mock.calls[4];
            expect(reconnectCall[0]).toBe('/api/v1/configmaps');
            expect(reconnectCall[1]).toEqual({
                labelSelector: CONFIG_LABELS.targetSecret
            });
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
