import io.activej.common.time.Stopwatch;
import io.activej.config.Config;
import io.activej.config.ConfigModule;
import io.activej.config.converter.ConfigConverters;
import io.activej.eventloop.Eventloop;
import io.activej.inject.annotation.Inject;
import io.activej.inject.annotation.Provides;
import io.activej.inject.module.Module;
import io.activej.inject.module.Modules;
import io.activej.launcher.Launcher;
import io.activej.promise.Promises;
import io.activej.redis.RedisClient;
import io.activej.service.ServiceGraphModule;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;


//[START EXAMPLE]
public class RedisPipeliningExample extends Launcher {
	// region setup
	public static final String REDIS_PROPERTIES = "redis.properties";
	public static final int NUMBER_OF_ENTRIES = 10_000;

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
		Stopwatch sw = Stopwatch.createUnstarted();
		eventloop.submit(() ->
				client.getConnection()
						// Clean up redis
						.then(connection -> connection.flushAll(false)

								// Add 10,000 entries without pipelining
								.then(() -> {
									sw.start();
									return Promises.loop(0,
											i -> i < NUMBER_OF_ENTRIES,
											i -> connection.set("key_" + i, "value_" + i)
													.map($ -> i + 1));
								})
								.whenResult(sw::stop)
								.then(connection::dbsize)
								.whenResult(numberOfKeys -> System.out.println("Populated " + numberOfKeys +
										" keys without pipelining in " + sw))

								// Add 10,000 entries using pipelining
								.then(() -> {
									sw.reset();
									sw.start();
									return Promises.all(IntStream.range(0, NUMBER_OF_ENTRIES-1)
											.mapToObj(i -> connection.set("key_" + i, "value_" + i)));
								})
								.whenResult(sw::stop)
								.then(connection::dbsize)
								.whenResult(numberOfKeys -> System.out.println("Populated " + numberOfKeys +
										" keys using pipelining in " + sw))
						))
				.get();
	}

	public static void main(String[] args) throws Exception {
		RedisPipeliningExample example = new RedisPipeliningExample();
		example.launch(args);
	}
}
//[END EXAMPLE]
