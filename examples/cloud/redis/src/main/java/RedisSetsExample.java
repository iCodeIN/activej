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
public class RedisSetsExample extends Launcher {
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

								// Create a set with some elements
								.then(() -> connection.sadd("set", "value1", "value2", "value3"))
								.then(() -> connection.smembers("set"))
								.whenResult(set -> System.out.println("\nSet: " + set))

								// Get length of a set
								.then(() -> connection.scard("set"))
								.whenResult(len -> System.out.println("Length of a set: " + len))

								// Pop element from a set
								.then(() -> connection.spop("set"))
								.whenResult(popped -> System.out.println("Popped value: '" + popped + '\''))
								.then(() -> connection.smembers("set"))
								.whenResult(set -> System.out.println("Set: " + set))

								// Is value a member of set
								.then(() -> connection.sismember("set", "value1"))
								.whenResult(isMember -> System.out.println("'value1' is member of set: " + isMember))
								.then(() -> connection.sismember("set", "value2"))
								.whenResult(isMember -> System.out.println("'value2' is member of set: " + isMember))
								.then(() -> connection.sismember("set", "value3"))
								.whenResult(isMember -> System.out.println("'value3' is member of set: " + isMember))

								// Get random element
								.then(() -> connection.srandmember("set"))
								.whenResult(randomElement -> System.out.println("Random element: '" + randomElement + "'\n"))
						))
				.get();
	}

	public static void main(String[] args) throws Exception {
		RedisSetsExample example = new RedisSetsExample();
		example.launch(args);
	}
}
//[END EXAMPLE]
