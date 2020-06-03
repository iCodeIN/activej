package io.activej.aggregation;

import io.activej.aggregation.ot.AggregationDiff;
import io.activej.aggregation.ot.AggregationStructure;
import io.activej.codegen.DefiningClassLoader;
import io.activej.datastream.StreamSupplier;
import io.activej.eventloop.Eventloop;
import io.activej.remotefs.LocalFsClient;
import io.activej.test.rules.ByteBufRule;
import io.activej.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.activej.aggregation.fieldtype.FieldTypes.ofInt;
import static io.activej.aggregation.fieldtype.FieldTypes.ofString;
import static io.activej.aggregation.measure.Measures.union;
import static io.activej.common.collection.CollectionUtils.set;
import static io.activej.promise.TestUtils.await;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

public class InvertedIndexTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	public static class InvertedIndexQueryResult {
		public String word;
		public Set<Integer> documents;

		@SuppressWarnings("unused")
		public InvertedIndexQueryResult() {
		}

		public InvertedIndexQueryResult(String word, Set<Integer> documents) {
			this.word = word;
			this.documents = documents;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			InvertedIndexQueryResult that = (InvertedIndexQueryResult) o;

			if (!Objects.equals(word, that.word)) return false;
			return Objects.equals(documents, that.documents);

		}

		@Override
		public int hashCode() {
			int result = word != null ? word.hashCode() : 0;
			result = 31 * result + (documents != null ? documents.hashCode() : 0);
			return result;
		}

		@Override
		public String toString() {
			return "InvertedIndexQueryResult{" +
					"word='" + word + '\'' +
					", documents=" + documents +
					'}';
		}
	}

	@Test
	public void testInvertedIndex() throws Exception {
		Executor executor = Executors.newCachedThreadPool();
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		DefiningClassLoader classLoader = DefiningClassLoader.create();
		Path path = temporaryFolder.newFolder().toPath();
		AggregationChunkStorage<Long> aggregationChunkStorage = RemoteFsChunkStorage.create(eventloop, ChunkIdCodec.ofLong(), new IdGeneratorStub(), LocalFsClient.create(eventloop, executor, path));

		AggregationStructure structure = AggregationStructure.create(ChunkIdCodec.ofLong())
				.withKey("word", ofString())
				.withMeasure("documents", union(ofInt()));

		Aggregation aggregation = Aggregation.create(eventloop, executor, classLoader, aggregationChunkStorage, structure)
				.withTemporarySortDir(temporaryFolder.newFolder().toPath());

		StreamSupplier<InvertedIndexRecord> supplier = StreamSupplier.of(
				new InvertedIndexRecord("fox", 1),
				new InvertedIndexRecord("brown", 2),
				new InvertedIndexRecord("fox", 3));

		doProcess(aggregationChunkStorage, aggregation, supplier);

		supplier = StreamSupplier.of(
				new InvertedIndexRecord("brown", 3),
				new InvertedIndexRecord("lazy", 4),
				new InvertedIndexRecord("dog", 1));

		doProcess(aggregationChunkStorage, aggregation, supplier);

		supplier = StreamSupplier.of(
				new InvertedIndexRecord("quick", 1),
				new InvertedIndexRecord("fox", 4),
				new InvertedIndexRecord("brown", 10));

		doProcess(aggregationChunkStorage, aggregation, supplier);

		AggregationQuery query = AggregationQuery.create()
				.withKeys("word")
				.withMeasures("documents");

		List<InvertedIndexQueryResult> list = await(aggregation.query(query, InvertedIndexQueryResult.class, DefiningClassLoader.create(classLoader))
				.toList());

		List<InvertedIndexQueryResult> expectedResult = asList(
				new InvertedIndexQueryResult("brown", set(2, 3, 10)),
				new InvertedIndexQueryResult("dog", set(1)),
				new InvertedIndexQueryResult("fox", set(1, 3, 4)),
				new InvertedIndexQueryResult("lazy", set(4)),
				new InvertedIndexQueryResult("quick", set(1)));

		assertEquals(expectedResult, list);
	}

	public void doProcess(AggregationChunkStorage<Long> aggregationChunkStorage, Aggregation aggregation, StreamSupplier<InvertedIndexRecord> supplier) {
		AggregationDiff diff = await(supplier.streamTo(aggregation.consume(InvertedIndexRecord.class)));
		aggregation.getState().apply(diff);
		await(aggregationChunkStorage.finish(getAddedChunks(diff)));
	}

	private Set<Long> getAddedChunks(AggregationDiff aggregationDiff) {
		return aggregationDiff.getAddedChunks().stream().map(AggregationChunk::getChunkId).map(id -> (long) id).collect(toSet());
	}

}
