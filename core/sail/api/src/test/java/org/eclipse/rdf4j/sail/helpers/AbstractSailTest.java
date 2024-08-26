/*******************************************************************************
 * Copyright (c) 2019 Eclipse RDF4J contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Distribution License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * SPDX-License-Identifier: BSD-3-Clause
 *******************************************************************************/
package org.eclipse.rdf4j.sail.helpers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Unit tests for {@link AbstractSail}.
 *
 * @author Jeen Broekstra
 */
@State(Scope.Benchmark)
@Warmup(iterations = 5)
@BenchmarkMode({ Mode.AverageTime })
@Fork(value = 1, jvmArgs = { "-Xms1G", "-Xmx1G", "-XX:+UseG1GC" })
@Measurement(iterations = 5)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class AbstractSailTest {

	@Param({ "10", "50", "100", "500", "1000", "5000" })
	private int count;

	AbstractSail subject;

	private final Random random = new Random(43252333);

	@BeforeEach
	@Setup(Level.Iteration)
	public void setUp() {

		subject = new AbstractSail() {

			@Override
			public boolean isWritable() throws SailException {
				return false;
			}

			@Override
			public ValueFactory getValueFactory() {
				return SimpleValueFactory.getInstance();
			}

			@Override
			protected void shutDownInternal() throws SailException {
				// TODO Auto-generated method stub

			}

			@Override
			protected SailConnection getConnectionInternal() throws SailException {
				SailConnection connDouble = mock(SailConnection.class);
				doAnswer(f -> {
					subject.connectionClosed(connDouble);
					return null;
				}).when(connDouble).close();
				return connDouble;
			}

		};
	}

	@Test
	public void testAutoInitOnConnection() {
		assertThat(subject.isInitialized()).isFalse();
		SailConnection conn = subject.getConnection();
		assertThat(subject.isInitialized()).isTrue();
	}

	@Test
	public void testExplicitInitBeforeConnection() {
		assertThat(subject.isInitialized()).isFalse();
		subject.init();
		SailConnection conn = subject.getConnection();
		assertThat(subject.isInitialized()).isTrue();
	}

	@Test
	public void testExplicitInitTwice() {
		assertThat(subject.isInitialized()).isFalse();
		subject.init();
		subject.init();
		SailConnection conn = subject.getConnection();
		assertThat(subject.isInitialized()).isTrue();
	}

	@Test
	@Benchmark
	public void testConcurrentAutoInit() throws Exception {
		CountDownLatch latch = new CountDownLatch(count);

		for (int i = 0; i < count; i++) {
			Thread.ofVirtual().start(new SailGetConnectionTask(subject, latch));
		}

		if (!latch.await(30, TimeUnit.SECONDS)) {
			fail("possible deadlock detected");
		}
	}

	class SailGetConnectionTask implements Runnable {

		private final AbstractSail sail;
		private final CountDownLatch connectionObtained;

		public SailGetConnectionTask(AbstractSail sail, CountDownLatch connectionObtained) {
			this.sail = sail;
			this.connectionObtained = connectionObtained;
		}

		/*
		 * (non-Javadoc)
		 *
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public void run() {
			try {
				Thread.sleep(random.nextInt(1000));
				if ((random.nextInt() & 1) == 0) {
					sail.init(); // 50% of our runs do an explicit init
				}
				try (SailConnection conn = sail.getConnection()) {
					Thread.sleep(10);
				}
				if (random.nextInt(4) == 1) {
					sail.shutDown(); // roughly one in four do a shutdown follow by another get connection
					try (SailConnection conn = sail.getConnection()) {
						Thread.sleep(10);
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				this.connectionObtained.countDown();
			}
		}

	}

}
