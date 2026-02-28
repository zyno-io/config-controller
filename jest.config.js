module.exports = {
    transform: {
        '^.+\\.(ts|tsx)$': 'ts-jest'
    },
    testEnvironment: 'node',
    testMatch: ['**/*.spec.ts'],
    collectCoverage: false,
    watchman: false,
    reporters: ['default'],
    coverageReporters: ['text', 'lcov', 'cobertura'],
    testLocationInResults: true
};
