package mtday.radml.nexrad;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Arrays.asList;

public class NexradIterator implements AutoCloseable, Iterator<Path> {
    private static final Logger LOGGER = LoggerFactory.getLogger(NexradIterator.class);

    private final AmazonS3 s3;
    private final String bucket;
    private final String station;
    private final LocalDate startDate;
    private final LocalDate endDate;

    private Iterator<String> objectKeys;
    private Path path;

    public NexradIterator(Properties properties) {
        s3 = AmazonS3ClientBuilder.standard().withRegion(properties.getProperty("nexrad.s3.region")).build();
        bucket = properties.getProperty("nexrad.s3.bucket");
        station = properties.getProperty("nexrad.station");
        startDate = LocalDate.parse(properties.getProperty("nexrad.daterange.start"));
        endDate = LocalDate.parse(properties.getProperty("nexrad.daterange.end"));
        objectKeys = fetchObjectKeys();
    }

    private Iterator<String> fetchObjectKeys() {
        Set<String> objectKeys = new TreeSet<>();
        LocalDate currentDay = startDate;
        while (!currentDay.isAfter(endDate)) {
            objectKeys.addAll(fetchObjectKeysForDate(currentDay));
            currentDay = currentDay.plusDays(1);
        }
        return objectKeys.iterator();
    }

    private Set<String> fetchObjectKeysForDate(LocalDate date) {
        String prefix = DateTimeFormatter.ofPattern("yyyy/MM/dd").format(date) + "/" + station;

        Set<String> objectKeys = new TreeSet<>();
        ListObjectsV2Request request = new ListObjectsV2Request()
                .withBucketName(bucket)
                .withPrefix(prefix);
        ListObjectsV2Result result;
        do {
            result = s3.listObjectsV2(request);
            result.getObjectSummaries().stream()
                    .map(S3ObjectSummary::getKey)
                    .forEach(objectKeys::add);
            request.setContinuationToken(result.getNextContinuationToken());
        } while (result.isTruncated());
        return objectKeys;
    }

    private void deletePath() {
        if (path != null) {
            try {
                Files.delete(path);
                path = null;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Path fetchNextPath() {
        String objectKey = objectKeys.next();
        objectKeys.remove();

        List<String> keyParts = asList(objectKey.split("/"));
        String filename = keyParts.get(keyParts.size() - 1);
        Path path = FileSystems.getDefault().getPath(System.getProperty("java.io.tmpdir"), filename);

        S3Object object = s3.getObject(bucket, objectKey);
        try (S3ObjectInputStream inputStream = object.getObjectContent()) {
            Files.copy(inputStream, path, REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return path;
    }

    @Override
    public void close() {
        deletePath();
    }

    @Override
    public boolean hasNext() {
        return objectKeys.hasNext();
    }

    @Override
    public Path next() {
        deletePath();
        return fetchNextPath();
    }
}
