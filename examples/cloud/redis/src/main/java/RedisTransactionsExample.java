import io.activej.common.exception.ToDoException;
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
public class RedisTransactionsExample extends Launcher {
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
								.then(() -> {throw new ToDoException();})
						))
				.get();
	}

	public static void main(String[] args) throws Exception {
		RedisTransactionsExample example = new RedisTransactionsExample();
		example.launch(args);
	}
}
//[END EXAMPLE]
