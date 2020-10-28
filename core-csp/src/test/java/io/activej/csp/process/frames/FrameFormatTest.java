package io.activej.csp.process.frames;

import io.activej.bytebuf.ByteBuf;
import io.activej.bytebuf.ByteBufQueue;
import io.activej.bytebuf.ByteBufStrings;
import io.activej.common.MemSize;
import io.activej.csp.ChannelConsumer;
import io.activej.csp.ChannelSupplier;
import io.activej.csp.process.ChannelByteChunker;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static io.activej.csp.binary.BinaryChannelSupplier.UNEXPECTED_DATA_EXCEPTION;
import static io.activej.csp.process.frames.FrameFormats.*;
import static io.activej.promise.TestUtils.await;
import static io.activej.promise.TestUtils.awaitException;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assume.assumeFalse;

@RunWith(Parameterized.class)
public class FrameFormatTest {
	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	private static final byte[] RANDOM_MAGIC_NUMBER;

	static {
		Random random = ThreadLocalRandom.current();
		RANDOM_MAGIC_NUMBER = new byte[random.nextInt(20) + 1];
		random.nextBytes(RANDOM_MAGIC_NUMBER);
	}

	@Parameter()
	public String testName;

	@Parameter(1)
	public FrameFormat frameFormat;

	@Parameter(2)
	public boolean hasNoTrailingData;

	@Parameter(3)
	public boolean resetsAllowed;


	@Parameters(name = "{0}")
	public static Collection<Object[]> getParameters() {
		return Arrays.asList(
				new Object[]{"LZ4 format", LZ4FrameFormat.create(), false, true},
				new Object[]{"Legacy LZ4 format", LZ4LegacyFrameFormat.create(), false, true},

				new Object[]{"Size prefixed", FrameFormats.sizePrefixed(), false, true},
				new Object[]{"Identity", FrameFormats.identity(), true, true},

				new Object[]{"Compound: Encoded with LZ4, decoded with legacy LZ4", testCompound(LZ4FrameFormat.create(), LZ4LegacyFrameFormat.create()), false, true},
				new Object[]{"Compound: Encoded with legacy LZ4, decoded with LZ4", testCompound(LZ4LegacyFrameFormat.create(), LZ4FrameFormat.create()), false, true},
				new Object[]{"Compound: Encoded with legacy LZ4, decoded with two legacy LZ4s", testCompound(LZ4LegacyFrameFormat.create(), LZ4LegacyFrameFormat.create()), false, true},
				new Object[]{"Compound: Encoded with LZ4, decoded with two LZ4s", testCompound(LZ4FrameFormat.create(), LZ4FrameFormat.create()), false, true},
				new Object[]{"Compound: Encoded with LZ4, decoded with identity", testCompound(LZ4FrameFormat.create(), FrameFormats.identity()), true, true},

				new Object[]{"With random magic number: Size prefixed", withMagicNumber(sizePrefixed(), RANDOM_MAGIC_NUMBER), false, true},
				new Object[]{"With random magic number: Identity", withMagicNumber(identity(), RANDOM_MAGIC_NUMBER), true, false},
				new Object[]{"With random magic number: LZ4", withMagicNumber(LZ4FrameFormat.create(), RANDOM_MAGIC_NUMBER), false, true},
				new Object[]{"With random magic number: Legacy LZ4", withMagicNumber(LZ4LegacyFrameFormat.create(), RANDOM_MAGIC_NUMBER), false, true}
		);
	}

	@Test
	public void test() {
		//[START EXAMPLE]
		int buffersCount = 100;

		List<ByteBuf> buffers = IntStream.range(0, buffersCount).mapToObj($ -> createRandomByteBuf()).collect(toList());
		byte[] expected = buffers.stream().map(ByteBuf::slice).collect(ByteBufQueue.collector()).asArray();

		ChannelSupplier<ByteBuf> supplier = ChannelSupplier.ofList(buffers)
				.transformWith(ChannelByteChunker.create(MemSize.of(64), MemSize.of(128)))
				.transformWith(ChannelFrameEncoder.create(frameFormat))
				.transformWith(ChannelByteChunker.create(MemSize.of(64), MemSize.of(128)))
				.transformWith(ChannelFrameDecoder.create(frameFormat));

		ByteBuf collected = await(supplier.toCollector(ByteBufQueue.collector()));
		assertArrayEquals(expected, collected.asArray());
		//[END EXAMPLE]
	}

	@Test
	public void singleByte() {
		doTest("1".getBytes(), false, false);
		doTest("1".getBytes(), false, true);
		doTest("1".getBytes(), true, false);
		doTest("1".getBytes(), true, true);
	}

	@Test
	public void empty() {
		doTest(new byte[0], false, false);
		doTest(new byte[0], false, true);
		doTest(new byte[0], true, false);
		doTest(new byte[0], true, true);
	}

	@Test
	public void singleByteRepetition() {
		byte[] data = new byte[100 * 1024];
		Arrays.fill(data, (byte) 'a');
		doTest(data, false, false);
		doTest(data, false, true);

		data = new byte[1024];
		Arrays.fill(data, (byte) 'a');
		doTest(data, true, false);
		doTest(data, true, true);
	}

	@Test
	public void randomBytes() {
		byte[] data = new byte[100 * 1024];
		ThreadLocalRandom.current().nextBytes(data);
		doTest(data, false, false);
		doTest(data, false, true);

		data = new byte[1024];
		ThreadLocalRandom.current().nextBytes(data);
		doTest(data, true, false);
		doTest(data, true, true);
	}

	@Test
	public void trailingData() {
		assumeFalse(hasNoTrailingData);

		ChannelFrameEncoder compressor = ChannelFrameEncoder.create(frameFormat);
		ChannelFrameDecoder decompressor = ChannelFrameDecoder.create(frameFormat);
		ByteBufQueue queue = new ByteBufQueue();

		await(ChannelSupplier.of(ByteBufStrings.wrapAscii("TestData")).transformWith(compressor)
				.streamTo(ChannelConsumer.ofConsumer(queue::add)));

		// add trailing 0 - bytes
		queue.add(ByteBuf.wrapForReading(new byte[10]));

		Throwable e = awaitException(ChannelSupplier.of(queue.takeRemaining())
				.transformWith(decompressor)
				.streamTo(ChannelConsumer.ofConsumer(ByteBuf::recycle)));

		assertSame(UNEXPECTED_DATA_EXCEPTION, e);
	}

	private void doTest(byte[] data, boolean singleByteChunks, boolean resets) {
		if (!resetsAllowed) return;

		MemSize chunkSizeMin = singleByteChunks ?
				MemSize.of(1) :
				MemSize.bytes(ThreadLocalRandom.current().nextInt(1000) + 500);

		ChannelFrameEncoder encoder = ChannelFrameEncoder.create(frameFormat);
		ChannelFrameDecoder decoder = ChannelFrameDecoder.create(frameFormat);
		if (resets) {
			encoder = encoder.withEncoderResets();
			decoder = decoder.withDecoderResets();
		}
		ChannelSupplier<ByteBuf> byteBufChannelSupplier = ChannelSupplier.of(ByteBuf.wrapForReading(data))
				.transformWith(ChannelByteChunker.create(chunkSizeMin, chunkSizeMin))
				.map(buf -> ByteBuf.wrapForReading(buf.asArray()))
				.transformWith(encoder);

		ByteBuf compressed = await(byteBufChannelSupplier.toCollector(ByteBufQueue.collector()));

		ChannelSupplier<ByteBuf> supplier = ChannelSupplier.of(compressed)
				.transformWith(ChannelByteChunker.create(chunkSizeMin, chunkSizeMin))
				.map(buf -> ByteBuf.wrapForReading(buf.asArray()))
				.transformWith(decoder);

		ByteBuf collected = await(supplier.toCollector(ByteBufQueue.collector()));
		assertArrayEquals(data, collected.asArray());
	}

	// encodes with random, decodes in order
	private static FrameFormat testCompound(FrameFormat mainFormat, FrameFormat... formats) {
		List<FrameFormat> allFormats = new ArrayList<>();
		allFormats.add(mainFormat);
		allFormats.addAll(Arrays.asList(formats));

		FrameFormat compound = compound(mainFormat, formats);
		return new FrameFormat() {
			@Override
			public BlockEncoder createEncoder() {
				return allFormats.get(ThreadLocalRandom.current().nextInt(allFormats.size())).createEncoder();
			}

			@Override
			public BlockDecoder createDecoder() {
				return compound.createDecoder();
			}
		};
	}

	private static ByteBuf createRandomByteBuf() {
		ThreadLocalRandom random = ThreadLocalRandom.current();
		int offset = random.nextInt(10);
		int tail = random.nextInt(10);
		int len = random.nextInt(100);
		byte[] array = new byte[offset + len + tail];
		random.nextBytes(array);
		return ByteBuf.wrap(array, offset, offset + len);
	}
}
