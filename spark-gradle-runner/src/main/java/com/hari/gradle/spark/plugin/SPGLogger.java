package com.hari.gradle.spark.plugin;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A singleton unified logger for logging needs across all gradle tasks and
 * plugin classes invovled in the project.
 * 
 * @author harim
 *
 */
public class SPGLogger {

	private static final Logger logger = Logger.getLogger(SPGLogger.class.getName());
	static {
		String logLevel = System.getenv("logLevel");
		logger.setLevel(logLevel != null && !logLevel.isEmpty() ? Level.parse(logLevel) : Level.INFO);
	}

	private static final BiConsumer<String, Level> log = (msg, lev) -> logger.log(lev, msg);
	public static final Consumer<String> logInfo = msg -> log.accept(msg, Level.INFO);
	public static final Consumer<String> logError = msg -> log.accept(msg, Level.SEVERE);
	public static final Consumer<String> logWarn = msg -> log.accept(msg, Level.WARNING);
	public static final Consumer<String> logFine = msg -> log.accept(msg, Level.FINE);

	// safe-guarding from inquisitive consumers.
	private SPGLogger(String level) {
	}

	public static final BiFunction<String, String, String> PROPERTY_SET_VALUE = (property, value) -> String
			.format("%s property is set to %s", property, value);

}
