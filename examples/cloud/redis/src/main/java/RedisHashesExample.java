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


//[START EXAMPLE]
public class RedisHashesExample extends Launcher {
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

								// Create a hash with some elements
								.then(() -> connection.hmset("hash",
										"key1", "value1",
										"key2", "value2",
										"key3", "value3",
										"key4", "value4",
										"key5", "value5"
								))
								.then(() -> connection.hgetall("hash"))
								.whenResult(hash -> System.out.println("\nHash: " + hash))

								// Does hash field exists
								.then(() -> connection.hexists("hash", "key3"))
								.whenResult(exists -> System.out.println("Field 'key3' exists: " + exists))
								.then(() -> connection.hexists("hash", "key6"))
								.whenResult(exists -> System.out.println("Field 'key6' exists: " + exists))

								// Get value of a hash field
								.then(() -> connection.hget("hash", "key2"))
								.whenResult(value -> System.out.println("Value of field 'key2': " + value))
								.then(() -> connection.hget("hash", "key6"))
								.whenResult(value -> System.out.println("Value of field 'key6': " + value))

								// Get number of fields in a hash
								.then(() -> connection.hlen("hash"))
								.whenResult(numberOfFields -> System.out.println("Number of fields in a hash: " + numberOfFields))

								// Get all fields in a hash
								.then(() -> connection.hkeys("hash"))
								.whenResult(fields -> System.out.println("Fields of a hash: " + fields))

								// Get all values in a hash
								.then(() -> connection.hvals("hash"))
								.whenResult(values -> System.out.println("Values of a hash: " + values))

								// Get length of the value of a hash field
								.then(() -> connection.hstrlen("hash", "key1"))
								.whenResult(len -> System.out.println("Length of value in field 'key1': " + len))

								// delete fields
								.then(() -> connection.hdel("hash", "key1", "key3"))
								.then(() -> connection.hgetall("hash"))
								.whenResult(hash -> System.out.println("Hash: " + hash + '\n'))
						))
				.get();
	}

	public static void main(String[] args) throws Exception {
		RedisHashesExample example = new RedisHashesExample();
		example.launch(args);
	}
}
//[END EXAMPLE]
