package mtday.radml;

import mtday.radml.config.ConfigLoader;
import mtday.radml.nexrad.NexradIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Properties;

public class Runner {
    private static final Logger LOGGER = LoggerFactory.getLogger(Runner.class);

    public static void main(String... args) {
        Properties properties = ConfigLoader.getProperties();

        try (NexradIterator nexradIterator = new NexradIterator(properties)) {
            int count = 0;
            while (count++ < 3 && nexradIterator.hasNext()) {
                Path path = nexradIterator.next();
                LOGGER.info("Processing path: {}", path);
            }
        }
    }
}
