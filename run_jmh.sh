#!/bin/bash

mvn clean package -Pbenchmarks -DskipTests
java -jar ./core/sail/memory/target/jmh-benchmarks.jar ConcurrentQueryBenchmark
java -jar ./core/sail/memory/target/jmh-benchmarks.jar MemStatementListTestIT
java -jar ./compliance/repository/target/jmh-benchmarks.jar RepositoryFederatedServiceIntegrationTest
java -jar ./core/queryalgebra/evaluation/target/jmh-benchmarks.jar MinimalContextNowTest
java -jar ./core/sail/lucene/target/jmh-benchmarks.jar LuceneSailTest
java -jar ./core/sail/memory/target/jmh-benchmarks.jar ParallelQueryBenchmark
java -jar ./core/sail/memory/target/jmh-benchmarks.jar ParallelMixedReadWriteBenchmark
java -jar ./core/sail/memory/target/jmh-benchmarks.jar MemValueFactoryConcurrentBenchmark

java -jar ./core/sail/lmdb/target/jmh-benchmarks.jar OverflowBenchmarkConcurrent
java -jar ./core/sail/nativerdf/target/jmh-benchmarks.jar OverflowBenchmarkConcurrent
