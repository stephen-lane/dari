package com.psddev.dari.util;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.URL;
import javax.imageio.ImageIO;
import javax.imageio.spi.IIORegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JavaImageTiffReader {

    protected static final Logger LOGGER = LoggerFactory.getLogger(JavaImageTiffReader.class);

    public static BufferedImage readTiff(String url) throws IOException {

        try {
            if (!IIORegistry.lookupProviders(Class.forName(JavaImageEditor.TIFF_READER_CLASS)).hasNext()) {
                //Register TIFF support
                IIORegistry.getDefaultInstance().registerServiceProvider(Class.forName(JavaImageEditor.TIFF_READER_CLASS).newInstance());
            }

            BufferedImage tiffImage = ImageIO.read(new URL(url));

            //Convert to RGB
            BufferedImage bufferedImage = new BufferedImage(tiffImage.getWidth(), tiffImage.getHeight(), BufferedImage.TYPE_INT_ARGB);
            for (int x = 0; x < tiffImage.getWidth(); x++) {
                for (int y = 0; y < tiffImage.getHeight(); y++) {
                    bufferedImage.setRGB(x, y, tiffImage.getRGB(x, y));
                }
            }

            return bufferedImage;

        } catch (ClassNotFoundException ex) {
            LOGGER.error(JavaImageEditor.TIFF_READER_CLASS + " class not found");
            return null;
        } catch (InstantiationException ex) {
            LOGGER.error("Unable to instantiate an instance of " + JavaImageEditor.TIFF_READER_CLASS);
            return null;
        } catch (IllegalAccessException ex) {
            LOGGER.error("Unable to instantiate an instance of " + JavaImageEditor.TIFF_READER_CLASS);
            return null;
        }
    }
}
