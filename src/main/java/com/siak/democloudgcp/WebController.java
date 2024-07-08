package com.siak.democloudgcp;

import com.google.cloud.spring.storage.GoogleStorageResource;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.WritableResource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import static java.lang.String.format;

/**
 * A REST Controller that exposes read and write operations on a Google Cloud Storage file accessed
 * using the Spring Resource Abstraction.
 */
@RestController
public class WebController {
    private static final Logger LOGGER = LoggerFactory.getLogger(WebController.class);

    @Value("${bucket.name}")
    private String bucketName;

    @Value("gs://${gcs-resource-test-bucket:bkake-bucket}/my-file.txt")
    private Resource gcsFile;

    private final Storage storage;

    public WebController(Storage storage) {
        this.storage = storage;
    }

    @GetMapping("/file")
    public ResponseEntity<byte[]> getFile(@RequestParam("filename")  String filename,
                                          @RequestParam("directory") String directory) {
        String storagePath = format("%s/%s",directory,filename);
        var blobInfo = BlobInfo.newBuilder(bucketName, storagePath).build();

        var blobId = blobInfo.getBlobId();

        var blob = storage.get(blobId);

        LOGGER.info("blobInfo {} ", blobId);
        LOGGER.info("blobId {} ", blobId);
        LOGGER.info("blob {}", blob);

        if (blob != null) {
            return ResponseEntity.ok()
                    .contentType(MediaType.parseMediaType(blob.getContentType()))
                    .body(blob.getContent());
        }
        return ResponseEntity.notFound().build();
    }

    @GetMapping(value = "/")
    public String readGcsFile(@RequestParam("filename") Optional<String> filename)
            throws IOException {
        return StreamUtils.copyToString(
                filename.isPresent()
                        ? fetchResource(filename.get()).getInputStream()
                        : this.gcsFile.getInputStream(),
                Charset.defaultCharset())
                + "\n";
    }

    @PostMapping(value = "/")
    public String writeGcs(
            @RequestBody String data, @RequestParam("filename") Optional<String> filename)
            throws IOException {
        return updateResource(
                filename.map(this::fetchResource).orElse((GoogleStorageResource) this.gcsFile), data);
    }

    private String updateResource(Resource resource, String data) throws IOException {
        try (OutputStream os = ((WritableResource) resource).getOutputStream()) {
            os.write(data.getBytes());
        }
        return "file was updated\n";
    }

    private GoogleStorageResource fetchResource(String filename) {
        return new GoogleStorageResource(
                this.storage, format("gs://%s/%s", this.bucketName, filename));
    }
}