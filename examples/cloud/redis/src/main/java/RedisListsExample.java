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

import static io.activej.redis.InsertPosition.AFTER;
import static io.activej.redis.InsertPosition.BEFORE;


//[START EXAMPLE]
public class RedisListsExample extends Launcher {
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

								// Create a list with some elements
								.then(() -> connection.rpush("list", "value1", "value2", "value3", "value4"))
								.then(() -> connection.lrange("list"))
								.whenResult(list -> System.out.println("\nList: " + list))

								// Get length of a list
								.then(() -> connection.llen("list"))
								.whenResult(len -> System.out.println("Length of a list: " + len))

								// Get by index
								.then(() -> connection.lindex("list", 2))
								.whenResult(value -> System.out.println("Value: '" + value + '\''))

								// Insert after and before elements
								.then(() -> connection.linsert("list", AFTER, "value3", "value3.5"))
								.then(() -> connection.linsert("list", BEFORE, "value2", "value1.5"))
								.then(() -> connection.lrange("list"))
								.whenResult(list -> System.out.println("List: " + list))

								// Pop element from a list
								.then(() -> connection.lpop("list"))
								.whenResult(popped -> System.out.println("Popped value: '" + popped + '\''))
								.then(() -> connection.lrange("list"))
								.whenResult(list -> System.out.println("List: " + list))

								// Set element by index
								.then(() -> connection.lset("list", 0, "value1"))
								.then(() -> connection.lrange("list"))
								.whenResult(list -> System.out.println("List: " + list + '\n'))
						))
				.get();
	}

	public static void main(String[] args) throws Exception {
		RedisListsExample example = new RedisListsExample();
		example.launch(args);
	}
}
//[END EXAMPLE]
