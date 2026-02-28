import { K8sClient } from './k8s';
import { Manager } from './manager';

async function run() {
    const k8sClient = new K8sClient();
    const manager = new Manager(k8sClient);

    process.on('SIGTERM', () => manager.stop());
    process.on('SIGINT', () => manager.stop());

    await manager.start();
}

process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection at:', reason, 'promise:', promise);
    process.exit(1);
});

run().catch(err => {
    console.error(err);
    process.exit(1);
});
