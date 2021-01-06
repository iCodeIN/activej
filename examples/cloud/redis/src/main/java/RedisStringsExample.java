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
public class RedisStringsExample extends Launcher {
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

								// Set and get strings
								.then(() -> connection.set("my key", "my value"))
								.then(() -> connection.get("my key"))
								.whenResult(value -> System.out.println("\nValue: '" + value + '\''))

								// Append to a string
								.then(() -> connection.append("my key", " with appended data"))
								.then(() -> connection.get("my key"))
								.whenResult(value -> System.out.println("New value: '" + value + '\''))

								// Get a substring
								.then(() -> connection.getrange("my key", 3, 7))
								.whenResult(substring -> System.out.println("Substring: '" + substring + '\''))

								// Get length of string
								.then(() -> connection.strlen("my key"))
								.whenResult(len -> System.out.println("Length of string: " + len))

								// Overwrite part of string
								.then(() -> connection.setrange("my key", 23, "part"))
								.then(() -> connection.get("my key"))
								.whenResult(len -> System.out.println("Overwritten value: '" + len + '\''))

								// Multi get
								.then(() -> connection.set("other key", "other value"))
								.then(() -> connection.mget("my key", "other key"))
								.whenResult(values -> System.out.println("Values: " + values + "\n"))
						))
				.get();
	}

	public static void main(String[] args) throws Exception {
		RedisStringsExample example = new RedisStringsExample();
		example.launch(args);
	}
}
//[END EXAMPLE]
