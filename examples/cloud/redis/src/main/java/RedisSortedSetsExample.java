import io.activej.config.Config;
import io.activej.config.ConfigModule;
import io.activej.config.converter.ConfigConverters;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.inject.module.Modules;
import io.activej.launcher.Launcher;
import io.activej.redis.RedisClient;
import io.activej.service.ServiceGraphModule;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

import static io.activej.common.collection.CollectionUtils.map;


//[START EXAMPLE]
public class RedisSortedSetsExample extends Launcher {
	// region setup
	public static final String REDIS_PROPERTIES = "redis.properties";

	@Inject
	private RedisClient client;

	@Inject
	private Eventloop eventloop;

	@Provides
	Eventloop eventloop() {
		return Eventloop.create();
	}

	@Provides
	RedisClient client(Eventloop eventloop, Config config) {
		InetSocketAddress address = config.get(ConfigConverters.ofInetSocketAddress(), "redis.server");
		return RedisClient.create(eventloop, address);
	}

	@Provides
	Config config() {
		return Config.ofClassPathProperties(REDIS_PROPERTIES)
				.overrideWith(Config.ofSystemProperties("config"));
	}

	@Override
	protected Module getModule() {
		return Modules.combine(
				ServiceGraphModule.create(),
				ConfigModule.create()
						.withEffectiveConfigLogger()
		);
	}
	// endregion

	@Override
	protected void run() throws ExecutionException, InterruptedException {
		eventloop.submit(() ->
				client.getConnection()
						// Clean up redis
						.then(connection -> connection.flushAll(false)
								// Create a sorted set with some elements
								.then(() -> connection.zadd("zset", map(
										"value1", 1.23,
										"value2", 2.34,
										"value3", 0.12,
										"value4", 4.56,
										"value5", 3.45
								)))
								.then(() -> connection.zrange("zset"))
								.whenResult(zset -> System.out.println("\nSorted set: " + zset))

								// Range with scores
								.then(() -> connection.zrangeWithScores("zset"))
								.whenResult(zset -> System.out.println("Sorted set with scores: " + zset))

								// Get length of a sorted set
								.then(() -> connection.zcard("zset"))
								.whenResult(len -> System.out.println("Length of a sorted set: " + len))

								// Increment score
								.then(() -> connection.zincrby("zset", 3, "value2"))
								.then(() -> connection.zrangeWithScores("zset"))
								.whenResult(zset -> System.out.println("Sorted set with scores: " + zset))

								// Pop element with the highest score
								.then(() -> connection.zpopmax("zset"))
								.whenResult(popped -> System.out.println("Popped element with the highest score: " + popped))
								.then(() -> connection.zrangeWithScores("zset"))
								.whenResult(zset -> System.out.println("Sorted set with scores: " + zset))

								// Pop element with the lowest score
								.then(() -> connection.zpopmin("zset"))
								.whenResult(popped -> System.out.println("Popped element with the lowest score: " + popped))
								.then(() -> connection.zrangeWithScores("zset"))
								.whenResult(zset -> System.out.println("Sorted set with scores: " + zset))

								// Get rank of an element
								.then(() -> connection.zrank("zset", "value1"))
								.whenResult(rank -> System.out.println("Rank of 'value1': " + rank))
								.then(() -> connection.zrank("zset", "value4"))
								.whenResult(rank -> System.out.println("Rank of 'value4': " + rank))
								.then(() -> connection.zrank("zset", "value5"))
								.whenResult(rank -> System.out.println("Rank of 'value5': " + rank))

								// Get score of an element
								.then(() -> connection.zscore("zset", "value1"))
								.whenResult(rank -> System.out.println("Score of 'value1': " + rank))
								.then(() -> connection.zscore("zset", "value4"))
								.whenResult(rank -> System.out.println("Score of 'value4': " + rank))
								.then(() -> connection.zscore("zset", "value5"))
								.whenResult(rank -> System.out.println("Score of 'value5': " + rank + '\n'))
						))
				.get();
	}

	public static void main(String[] args) throws Exception {
		RedisSortedSetsExample example = new RedisSortedSetsExample();
		example.launch(args);
	}
}
//[END EXAMPLE]
