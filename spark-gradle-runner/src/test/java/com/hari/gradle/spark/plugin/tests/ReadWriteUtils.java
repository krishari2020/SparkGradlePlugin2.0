package com.hari.gradle.spark.plugin.tests;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Function;

import com.hari.gradle.spark.plugin.SPGLogger;

/**
 * Utility for reading content from file and writing content to file.
 * 
 * @author harim
 *
 */

public class ReadWriteUtils {

	static BiConsumer<File, String> writeToFile = (file, content) -> {
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
			writer.write(content);
			writer.flush();
		} catch (IOException iof) {
			SPGLogger.logError
					.accept(String.format("Failed with exception %s while writing to file %s", iof, file.getName()));
		}
	};

	static Function<File, String> readFromFile = file -> {
		try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
			StringBuffer readBuff = new StringBuffer();
			int readChar = reader.read();
			while (readChar != -1) {
				readBuff.append((char) readChar);
				readChar = reader.read();
			}
			String readContent = readBuff.toString();
			if (readContent == null || readContent.isEmpty())
				throw new RuntimeException("File content cannot be empty");
			return readBuff.toString();
		} catch (IOException io) {
			SPGLogger.logError
					.accept(String.format("Failed with exception %s while reading from file %s", io, file.getName()));
		}
		throw new RuntimeException(String.format("Could not read content from the file %", file));
	};

}
