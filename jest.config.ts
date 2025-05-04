import type { Config } from "jest";

export default async (): Promise<Config> => {
  return {
    testEnvironment: "node",
    transform: {
      "^.+.tsx?$": ["ts-jest", {}],
    },
    modulePathIgnorePatterns: ["<rootDir>/dist/", "jest.config.ts"],
    preset: "ts-jest",
    collectCoverage: true,
    collectCoverageFrom: ["<rootDir>/**/*.ts"],
    coverageThreshold: {
      global: {
        branches: 0,
        functions: 0,
        lines: 0,
        statements: 0,
      },
    },
  };
};
