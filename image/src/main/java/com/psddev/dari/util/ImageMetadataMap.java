package com.psddev.dari.util;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.FileImageInputStream;
import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.MemoryCacheImageInputStream;

import com.drew.imaging.ImageMetadataReader;
import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.Tag;

/**
 * Reads various metadata information from an image and makes it
 * available as a map.
 *
 * <p>For best results, it must be able to access the
 * <a href="http://www.drewnoakes.com/code/exif/">Metadata Extractor</a>.
 * If you use Maven, you should add the following dependency:</p>
 *
 * <blockquote><pre><code data-type="xml">{@literal
<dependency>
    <groupId>com.drewnoakes</groupId>
    <artifactId>metadata-extractor</artifactId>
    <version>2.8.1</version>
</dependency>}</code></pre></blockquote>
 *
 * <p>All errors will be {@linkplain #getErrors caught and stored}
 * so that each step in extracting the metadata has a chance to run. If you
 * want this class to throw an exception when it encounters any problems,
 * you should do so manually:</p>
 *
 * <blockquote><pre><code data-type="java">
ImageMetadataMap map = new ImageMetadataMap(file);
// Do something with the map.
List&lt;Throwable&gt; errors = map.getErrors();
if (!errors.isEmpty()) {
    throw new AggregateException(errors);
}</code></pre></blockquote>
 */
@SuppressWarnings("serial")
public class ImageMetadataMap extends HashMap<String, Object> {

    private final List<Throwable> errors = new ArrayList<Throwable>();

    /**
     * Creates an instance based on the given {@code input}.
     *
     * @param input Can't be {@code null}.
     */
    public ImageMetadataMap(InputStream input) {
        ByteArrayOutputStream tempOutput = new ByteArrayOutputStream();

        try {
            IoUtils.copy(input, tempOutput);
            populateDimension(new MemoryCacheImageInputStream(new ByteArrayInputStream(tempOutput.toByteArray())));
            populateMetadata(ImageMetadataReader.readMetadata(new BufferedInputStream(new ByteArrayInputStream(tempOutput.toByteArray()))));

        } catch (Throwable error) {
            errors.add(error);
        }
    }

    /**
     * Creates an instance based on the given {@code file}.
     *
     * @param file Can't be {@code null}.
     */
    public ImageMetadataMap(File file) {
        try {
            populateDimension(new FileImageInputStream(file));
            populateMetadata(ImageMetadataReader.readMetadata(file));

        } catch (Throwable error) {
            errors.add(error);
        }
    }

    // Populates the map with dimension information.
    private void populateDimension(ImageInputStream image) {
        try {
            try {
                Iterator<ImageReader> readers = ImageIO.getImageReaders(image);

                if (readers.hasNext()) {
                    ImageReader reader = readers.next();

                    try {
                        reader.setInput(image);
                        int index = reader.getMinIndex();
                        put("width", reader.getWidth(index));
                        put("height", reader.getHeight(index));

                    } finally {
                        reader.dispose();
                    }
                }

            } finally {
                image.close();
            }

        } catch (IOException error) {
            errors.add(error);
        }
    }

    // Populates the map based on the given {@code metadata}.
    private void populateMetadata(Metadata metadata) {
        for (Iterator<?> di = metadata.getDirectories().iterator(); di.hasNext();) {
            Directory directory = (Directory) di.next();
            Map<String, String> tags = new HashMap<String, String>();

            put(directory.getName(), tags);

            for (Iterator<?> ti = directory.getTags().iterator(); ti.hasNext();) {
                Tag tag = (Tag) ti.next();

                try {
                    tags.put(tag.getTagName(), directory.getDescription(tag.getTagType()));
                } catch (Exception error) {
                    errors.add(error);
                }
            }
        }
    }

    /**
     * Returns the error.
     *
     * @return Never {@code null}. Immutable.
     */
    public List<Throwable> getErrors() {
        return Collections.unmodifiableList(errors);
    }

    /**
     * Returns the last error.
     *
     * @return May be {@code null}.
     * @deprecated Use {@link #getErrors} instead.
     */
    @Deprecated
    public Throwable getError() {
        return errors.isEmpty() ? null : errors.get(errors.size() - 1);
    }
}
