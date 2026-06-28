# config-controller

Simple Kubernetes controller to create key/value Secrets from encrypted .env files stored in ConfigMaps.

Based on the [@zyno-io/config](https://github.com/zyno-io/node-config) package.

## Installation

```
helm repo add zyno-io https://zyno-io.github.io/charts
helm repo update
helm install --namespace kube-system config-controller zyno-io/config-controller
```

## Usage

### Source ConfigMap

Load an [encrypted .env file](https://github.com/zyno-io/node-config?tab=readme-ov-file#setup) into a ConfigMap. Set the following labels:

- `config.zyno.io/decryption-secret: dotenv-crypto-secrets`
- `config.zyno.io/decryption-secret-key: CONFIG_SECRET` (optional, defaults to `CONFIG_DECRYPTION_SECRET`)
- `config.zyno.io/source-key: env_content` (optional, defaults to `.env`)
- `config.zyno.io/target-secret: myapp-config`

The legacy `config.s24.dev/*` labels are still accepted for backwards compatibility, but new manifests should use `config.zyno.io/*`.

For example:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
    name: myapp-dotenv
    namespace: myapp
    labels:
        config.zyno.io/decryption-secret: myapp-dotenv-crypto
        config.zyno.io/target-secret: myapp-config
data:
    .env: |
        TWILIO_ACCOUNT_SID=AC123456
        TWILIO_AUTH_TOKEN_SECRET=$$[AQJLlkLEOjifkSWRHozwOK78xJfym11/utjD7NZwbYXOUTMMXHg+Fa34wt/ytB4LRB2kiD6qXSYTQQLPYRmxN+1/VcvWCATWPUXJEN+pl8MiaO5boOGMYqcTT9JVUQ+dyEZelJkR+fuhzAeoANKyicPFwYa7DiLRwUlLxca/7lnEiROzrh1YNtvWPM0+J3yjjh/zbwbRUWCVFRcP/jmToE5EGifGYhpSjzY004LDWNfF8fKiotZiISMXq8vbDBBpmYugmkHy6Q+DXMIoVsRhg/jY1LSO8ycNaE8eAjgS05tjnXo35Nx9Wr+QSKAU99+M0yK3zfq7nSnIfVQ7IRQXNV4N2Dte02ZX+AkPwNg/mPeWXD+Acnxzu2KDi4R9nmb1Qnk6VJ+BlejbtO+KhGexkDF9a2pvZyN+LDQM3c1OfL/WpqdIZkSsg7fhDWHYnTGUlr1tOxPndptc6im65Kq05/0ynB/e04HMopDz1EmkSXVV]
```

You could, for example, do this as part of a Helm deployment:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
    name: myapp-dotenv
    labels:
        config.zyno.io/decryption-secret: { { .Values.dotenv.decryptionSecret } }
        config.zyno.io/target-secret: { { .Values.dotenv.targetSecret } }
data:
    .env: |
        {{ .Values.dotenv.content | nindent 4 }}
```

### Decryption Secret

Create a secret with your decryption secret:

```yaml
apiVersion: v1
kind: Secret
metadata:
    name: myapp-dotenv-crypto
    namespace: myapp
type: Opaque
data:
    CONFIG_DECRYPTION_SECRET: >-
        TFlJRXZn...long decryption secret...xZRhXMcQ
```

Legacy entries named `CONFIG_DECRYPTION_KEY` are still supported when `config.zyno.io/decryption-secret-key` is unset.

### 🪄 Auto-Generated Config Secret

The controller will automatically generate a secret with the keys & decrypted values from the raw .env content:

```yaml
apiVersion: v1
kind: Secret
metadata:
    name: myapp-config
    namespace: myapp
    labels:
        config.zyno.io/source-configmap: myapp-dotenv
        config.zyno.io/source-configmap-version: '123456'
type: Opaque
stringData:
    TWILIO_ACCOUNT_SID: AC123456
    TWILIO_AUTH_TOKEN_SECRET: SecretToken
```

You can now mount this secret into your workload:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
    name: myapp
spec:
    selector:
        matchLabels:
            app: myapp
    template:
        metadata:
            labels:
                app: myapp
        spec:
            containers:
                - name: myapp
                  image: myapp:latest

                  # all variables
                  envFrom:
                      - secretRef:
                            name: myapp-config

                  # select variables
                  env:
                      - name: TWILIO_AUTH_TOKEN_SECRET
                        valueFrom:
                            secretKeyRef:
                                name: myapp-config
                                key: TWILIO_AUTH_TOKEN_SECRET
```

The config secret will be automatically updated any time the source ConfigMap is updated, and will be automatically deleted when the ConfigMap is deleted.
