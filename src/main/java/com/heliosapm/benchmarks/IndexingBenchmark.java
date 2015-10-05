/*
 * Copyright (c) 2005, 2014, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package com.heliosapm.benchmarks;

import java.io.File;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.charset.Charset;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import jsr166y.ThreadLocalRandom;
import net.openhft.chronicle.map.ChronicleMap;
import net.openhft.chronicle.map.ChronicleMapBuilder;
import net.openhft.lang.io.Bytes;
import net.openhft.lang.io.serialization.BytesMarshaller;

import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.hash.PrimitiveSink;
import com.heliosapm.utils.config.ConfigurationHelper;
import com.heliosapm.utils.time.SystemClock;
import com.heliosapm.utils.time.SystemClock.ElapsedTime;

/**
 * <p>Title: IndexingBenchmark</p>
 * <p>Description: </p> 
 * <p>Company: Helios Development Group LLC</p>
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>com.heliosapm.benchmarks.IndexingBenchmark</code></p>
 */
public class IndexingBenchmark {
		public static final int CORES = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
		public static final int SAMPLE_SIZE = ConfigurationHelper.getIntSystemThenEnvProperty("sample.size", 1000000);
		public static final int INIT_THREADS = ConfigurationHelper.getIntSystemThenEnvProperty("init,threads", SAMPLE_SIZE/CORES);
		public static final int LOOPS = ConfigurationHelper.getIntSystemThenEnvProperty("loops", 20);
		//private static final long[] randomIndexes = new long[SAMPLE_SIZE];
		
		
		private static final ThreadLocalRandom random = ThreadLocalRandom.current();
		/** The UTF8 character set */
		public static final Charset UTF8 = Charset.forName("UTF8");
		/** The hashing function to compute hashes for metric names */
		public static final HashFunction METRIC_NAME_HASHER = Hashing.murmur3_128();

		
		
		
		@State(Scope.Group)
    public static class NonBlockState {
			public NonBlockingHashMapLong<String> samples = null;
				//new NonBlockingHashMapLong<String>(SAMPLE_SIZE, false);
			public NonBlockingHashMapLong<String> output = null;
				//new NonBlockingHashMapLong<String>(SAMPLE_SIZE, true);
			
			@Setup(Level.Trial)
			public void setup() {
				log("Initializing....");
				ElapsedTime et = SystemClock.startClock();
				samples = new NonBlockingHashMapLong<String>(SAMPLE_SIZE, true);
				output = new NonBlockingHashMapLong<String>(SAMPLE_SIZE, true);
				for(long x = 0; x < SAMPLE_SIZE; x++) {
					samples.put(x, UUID.randomUUID().toString());
				}
				log(et.printAvg("UUIDs Generated", SAMPLE_SIZE));
				
			}
	    @TearDown
	    public void clear() {
	    	log("Output Size: %s", output.size());
	    	output.clear();
	    	System.gc();
	    	printJVMStats();
	    }			
    }

		@State(Scope.Group)
    public static class ChronicleState {
			public ChronicleMap<Long, String> samples = null;
			public ChronicleMap<Long, String> output = null;
			
			public static final BytesMarshaller<Long> keyMarshaller = new BytesMarshaller<Long>() {
				@Override
				public Long read(final Bytes bytes) {
					return bytes.readLong();
				}
				@Override
				public Long read(final Bytes bytes, final Long defaultValue) {
					return bytes.readLong();
				}
				@Override
				public void write(final Bytes bytes, final Long arg) {
					bytes.writeLong(arg.longValue());
				}				
			};
			
			public static final BytesMarshaller<String> valueMarshaller = new BytesMarshaller<String>() {
				@Override
				public String read(final Bytes bytes) {
					return bytes.readUTF();
				}
				@Override
				public String read(final Bytes bytes, final String value) {
					return bytes.readUTF();
				}
				@Override
				public void write(final Bytes bytes, final String value) {
					bytes.writeUTF(value);
				}				
			};
			
			protected final File sampleFile = new File("/home/nwhitehead/sd-data/chronicle/samples.db");
			protected final File outputFile = new File("/home/nwhitehead/sd-data/chronicle/output.db");
			
			@Setup(Level.Trial)
			public void setup() {
				log("Initializing....");
				sampleFile.delete();
				outputFile.delete();
				try {
					ElapsedTime et = SystemClock.startClock();
					samples = ChronicleMapBuilder.of(Long.class, String.class)
							.keyMarshaller(keyMarshaller)						
							.valueMarshaller(valueMarshaller)
							.constantKeySizeBySample(1L)
							.entries(SAMPLE_SIZE)
							.immutableKeys()
							.valueMarshaller(valueMarshaller)
							.create();
//							.createPersistedTo(sampleFile);
					output = ChronicleMapBuilder.of(Long.class, String.class)
							.keyMarshaller(keyMarshaller)						
							.valueMarshaller(valueMarshaller)
							.constantKeySizeBySample(1L)
							.entries(SAMPLE_SIZE)
							.immutableKeys()
							.valueMarshaller(valueMarshaller)
							.create();
//							.createPersistedTo(outputFile);
					for(long x = 0; x < SAMPLE_SIZE; x++) {
						samples.put(x, UUID.randomUUID().toString());
					}
					log(et.printAvg("UUIDs Generated", SAMPLE_SIZE));
				} catch (Exception ex) {
					throw new RuntimeException("Failed to initialize ChronicleState", ex);
				}
			}
	    @TearDown
	    public void clear() {
	    	log("Output Size: %s", output.size());
	    	samples.close();
	    	output.close();
	    	System.gc();
	    	printJVMStats();
	    }			
    }
		
		
		
		public static void main(String[] args) {
			log("IndexingBenchmark");
			final IndexingBenchmark benchmark = new IndexingBenchmark();
			final ElapsedTime et = SystemClock.startClock();
			final NonBlockState mystate = new NonBlockState();
			benchmark.testIndexing(mystate);
			log(et.printAvg("Indexing Benchmark", SAMPLE_SIZE * LOOPS));
			mystate.clear();
		}
	
	
    //@Benchmark
    public void testIndexing(final NonBlockState state) {
    	state.output.clear();
        for(int i = 0; i < LOOPS; i++) {
        	for(int x = 0; x < SAMPLE_SIZE; x++) {
        		final String v = state.samples.get(randomLong());
        		final long hash = hashCode(v);
        		final String ov = state.output.putIfAbsent(hash, v);
        		
//        		if(ov!=null && !ov.equals(v)) {
//        			final String msg = "Hash Collision: [" + v + "] vs. [" + ov + "] for hash [" + hash + "]";
//        			System.err.println(msg);
//        			throw new RuntimeException(msg);
//        		}
        	}
//        	for(long index : randomIndexes) {
//        		final String v = samples.get(index);
//        		final long hash = hashCode(v);
//        		output.putIfAbsent(hash, v);
//        	}
        }
    }
    
    @Group("NonBlockingHashMap")    
    @Fork(2)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
//    @GroupThreads(3)
    @OperationsPerInvocation(100)
//    @Benchmark
    public void testNonBlockingHashMap(final NonBlockState state, final Blackhole blackhole) {
    	for(int i = 0; i < 100; i++) {
    		final String v = state.samples.get(randomLong());
    		final long hash = hashCode(v);
    		final String ov = state.output.putIfAbsent(hash, v);
    		blackhole.consume(ov);
    		if(ov!=null && !ov.equals(v)) {
    			final String msg = "Hash Collision: [" + v + "] vs. [" + ov + "] for hash [" + hash + "]";
    			System.err.println(msg);
    			throw new RuntimeException(msg);
    		}    		
    	}
    }
    
    @Group("Chronicle")    
    @Fork(2)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
//    @GroupThreads(3)
    @OperationsPerInvocation(100)
    @Benchmark
    public void testChronicle(final ChronicleState state, final Blackhole blackhole) {
    	for(int i = 0; i < 100; i++) {
    		final String v = state.samples.get(randomLong());
    		final long hash = hashCode(v);
    		final String ov = state.output.putIfAbsent(hash, v);
    		blackhole.consume(ov);
    		if(ov!=null && !ov.equals(v)) {
    			final String msg = "Hash Collision: [" + v + "] vs. [" + ov + "] for hash [" + hash + "]";
    			System.err.println(msg);
    			throw new RuntimeException(msg);
    		}    		
    	}
    }
    
    
    public long hashCode(final String v) {
    	return METRIC_NAME_HASHER.hashObject(v, NameIndexingFunnel.INSTANCE).padToLong();
    }
    
    public static void log(final Object fmt, final Object...args) {
    	System.out.println(String.format(fmt.toString(), args));
    }
    
    public static long randomLong() {
    	return Math.abs(random.nextInt(0, SAMPLE_SIZE));
    }
    
  	public static enum NameIndexingFunnel implements Funnel<String> {
      /** The funnel instance */
      INSTANCE;
      @Override
  		public void funnel(final String v, final PrimitiveSink into) {
      	into.putString(v, UTF8);
      }
    }
    
  	public static void printJVMStats() {
  		log("Heap Usage:" + ManagementFactory.getMemoryMXBean().getHeapMemoryUsage());
  		for(final GarbageCollectorMXBean gc: ManagementFactory.getGarbageCollectorMXBeans()) {
  			log("GC [%s]: collections: [%s], time[%s]", gc.getName(), gc.getCollectionCount(), gc.getCollectionTime());
  		}
  	}

}
